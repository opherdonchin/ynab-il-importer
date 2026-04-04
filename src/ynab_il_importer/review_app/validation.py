from __future__ import annotations

from typing import Any

import pandas as pd
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
TARGET_MATCH_ACTIONS = {"keep_match", "create_source"}
SOURCE_DELETE_ACTIONS = {"delete_source", "delete_both"}
TARGET_DELETE_ACTIONS = {"delete_target", "delete_both"}


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


def normalize_decision_action(value: Any) -> str:
    text = _text(value)
    return text or NO_DECISION


def _truthy(value: Any) -> bool:
    return _text(value).casefold() in TRUE_VALUES or bool(value) is True


def _selected_value(row: pd.Series, field: str, *, side: str) -> str:
    side_key = f"{side}_{field}_selected"
    if side_key in row.index:
        value = _text(row.get(side_key))
    elif side == "target":
        value = _text(row.get(f"{field}_selected"))
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
    df: pd.DataFrame,
    component_map: dict[Any, int],
    *,
    row_errors_by_index: dict[Any, list[str]] | None = None,
) -> dict[int, list[str]]:
    component_errors: dict[int, list[str]] = {}
    if not component_map:
        return component_errors

    component_series = pd.Series(component_map).reindex(df.index)
    first_index_by_component: dict[int, Any] = {}
    for idx in df.index:
        label = component_map.get(idx)
        if label is None or label in first_index_by_component:
            continue
        first_index_by_component[label] = idx

    for label, start_idx in first_index_by_component.items():
        component_mask = component_series.eq(label).fillna(False)
        component_errors[label] = review_component_errors(
            df,
            start_idx,
            component_mask=component_mask,
            row_errors_by_index=row_errors_by_index,
        )

    return component_errors


def component_error_lookup(df: pd.DataFrame) -> dict[Any, list[str]]:
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


def validate_row(row: pd.Series) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    source_payee = _selected_value(row, "payee", side="source")
    source_category = _selected_value(row, "category", side="source")
    target_payee = _selected_value(row, "payee", side="target")
    target_category = _selected_value(row, "category", side="target")
    action = normalize_decision_action(row.get("decision_action", ""))
    reviewed = _truthy(row.get("reviewed", False))
    workflow_type = _text(row.get("workflow_type")).casefold()
    source_category_required = not model.is_transfer_payee(source_payee)
    category_required = not model.is_transfer_payee(target_payee)
    source_no_category_required = model.is_no_category_required(source_category)
    target_no_category_required = model.is_no_category_required(target_category)
    update_maps = parse_update_maps(row.get("update_maps", ""))

    if reviewed and action == NO_DECISION:
        errors.append("reviewed row cannot have No decision")
    if workflow_type == "institutional" and action in SOURCE_MUTATION_ACTIONS:
        errors.append(f"{action} is not allowed for institutional sources")
    if action == "create_source":
        if not source_payee:
            errors.append("missing source payee")
        if source_category_required and (not source_category or source_no_category_required):
            errors.append("missing source category")
    if action == "create_target":
        if not target_payee:
            errors.append("missing target payee")
        if category_required and (not target_category or target_no_category_required):
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


def precompute_row_errors(df: pd.DataFrame) -> dict[Any, list[str]]:
    return {
        idx: validate_row(row)[0]
        for idx, row in df.iterrows()
    }


def review_component_errors(
    df: pd.DataFrame,
    start_idx: Any,
    *,
    component_mask: pd.Series | None = None,
    row_errors_by_index: dict[Any, list[str]] | None = None,
) -> list[str]:
    if component_mask is None:
        component_mask = connected_component_mask(df, start_idx)
    else:
        component_mask = component_mask.reindex(df.index, fill_value=False)

    component = df.loc[component_mask].copy()
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
        row_errors = (
            row_errors_by_index.get(idx, [])
            if row_errors_by_index is not None
            else validate_row(row)[0]
        )
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


def blocker_label(
    row: pd.Series,
    *,
    component_errors: list[str],
    uncategorized: bool,
    row_errors: list[str] | None = None,
) -> str:
    if row_errors is None:
        row_errors = validate_row(row)[0]
    action = normalize_decision_action(row.get("decision_action", ""))
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


def allowed_decision_actions(row: pd.Series) -> list[str]:
    workflow_type = str(row.get("workflow_type", "") or "").strip().casefold()
    source_present = _truthy(row.get("source_present", False))
    target_present = _truthy(row.get("target_present", False))

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
        review_state.apply_row_edit(
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
            review_state.apply_row_edit(
                reverted,
                idx,
                reviewed=False,
                component_map=component_map,
            )
        unique_errors = list(dict.fromkeys(errors))
        return reverted, unique_errors

    return updated, []


def apply_review_state_best_effort(
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
        updated, errors = apply_review_state(
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
