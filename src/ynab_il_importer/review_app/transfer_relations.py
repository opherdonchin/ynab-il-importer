from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from typing import Any

import polars as pl

import ynab_il_importer.review_app.model as review_model


_RELATION_SCHEMA: dict[str, pl.DataType] = {
    "transfer_relation_id": pl.String,
    "relation_kind": pl.String,
    "relation_status": pl.String,
    "date": pl.String,
    "amount_abs_ils": pl.Float64,
    "account_a": pl.String,
    "account_b": pl.String,
    "row_positions": pl.List(pl.Int64),
    "source_row_ids": pl.List(pl.String),
    "target_row_ids": pl.List(pl.String),
    "account_a_source_present": pl.Boolean,
    "account_a_target_present": pl.Boolean,
    "account_b_source_present": pl.Boolean,
    "account_b_target_present": pl.Boolean,
    "peer_review_row_present": pl.Boolean,
    "ambiguous_relation": pl.Boolean,
}


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _normalize_bool(value: Any) -> bool:
    return _normalize_text(value).casefold() in {"true", "1", "yes", "y"}


def _optional_bool(value: Any) -> bool | None:
    text = _normalize_text(value).casefold()
    if text == "":
        return None
    return text in {"true", "1", "yes", "y"}


def _parse_float(value: Any) -> float:
    text = _normalize_text(value)
    if not text:
        return 0.0
    try:
        parsed = float(text)
    except ValueError:
        return 0.0
    return 0.0 if math.isnan(parsed) else parsed


def _row_account_name(row: dict[str, Any]) -> str:
    for key in ("account_name", "target_account", "source_account"):
        value = _normalize_text(row.get(key))
        if value:
            return value
    return ""


def _transfer_peer_account_name(row: dict[str, Any]) -> str:
    for key in (
        "target_payee_selected",
        "target_payee_current",
        "source_payee_selected",
        "source_payee_current",
        "payee_selected",
    ):
        peer = review_model.transfer_target_account_name(row.get(key))
        if peer:
            return peer
    return ""


def _transfer_date(row: dict[str, Any]) -> str:
    for key in ("source_date", "target_date", "date"):
        value = _normalize_text(row.get(key))
        if value:
            return value
    return ""


def _row_signed_amount(row: dict[str, Any]) -> float:
    return _parse_float(row.get("inflow_ils")) - _parse_float(row.get("outflow_ils"))


def _row_transfer_kind(row: dict[str, Any]) -> str:
    fingerprint = _normalize_text(row.get("fingerprint")).casefold()
    memo = _normalize_text(row.get("memo")).casefold()
    if "card payment" in fingerprint or "card payment" in memo:
        return "card_payment"

    current_on_budget = (
        _optional_bool(row.get("target_account_on_budget"))
        if row.get("target_account_on_budget") is not None
        else _optional_bool(row.get("source_account_on_budget"))
    )
    transfer_target_on_budget = (
        _optional_bool(row.get("target_transfer_account_on_budget"))
        if row.get("target_transfer_account_on_budget") is not None
        else _optional_bool(row.get("source_transfer_account_on_budget"))
    )
    if current_on_budget is not None and transfer_target_on_budget is not None:
        if current_on_budget and transfer_target_on_budget:
            return "internal_budget"
        return "budget_boundary"
    return "transfer"


def _empty_relation_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            name: pl.Series(name, [], dtype=dtype)
            for name, dtype in _RELATION_SCHEMA.items()
        }
    )


def build_transfer_relation_frame(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return _empty_relation_frame()

    frame = df if "_row_pos" in df.columns else df.with_row_index("_row_pos")
    rows = frame.to_dicts()
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        account_name = _row_account_name(row)
        peer_account = _transfer_peer_account_name(row)
        date = _transfer_date(row)
        amount_abs = round(abs(_row_signed_amount(row)), 2)
        row_pos = row.get("_row_pos")
        if (
            not isinstance(row_pos, int)
            or not account_name
            or not peer_account
            or account_name == peer_account
            or not date
            or amount_abs <= 0
        ):
            continue
        account_a, account_b = sorted([account_name, peer_account])
        key = (account_a, account_b, date, f"{amount_abs:.2f}")
        grouped[key].append(
            {
                "_row_pos": row_pos,
                "_row_account": account_name,
                "_source_present": _normalize_bool(row.get("source_present")),
                "_target_present": _normalize_bool(row.get("target_present")),
                "_source_row_id": _normalize_text(row.get("source_row_id")),
                "_target_row_id": _normalize_text(row.get("target_row_id")),
                "_relation_kind": _row_transfer_kind(row),
            }
        )

    relation_rows: list[dict[str, Any]] = []
    for (account_a, account_b, date, amount_text), members in grouped.items():
        by_account: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for member in members:
            by_account[_normalize_text(member.get("_row_account"))].append(member)

        ambiguous_relation = (
            len(members) > 2 or any(len(account_members) > 1 for account_members in by_account.values())
        )
        peer_review_row_present = len(by_account) > 1
        any_source_present = any(bool(member.get("_source_present")) for member in members)
        if ambiguous_relation:
            relation_status = "ambiguous_multiple_review_rows"
        elif peer_review_row_present:
            relation_status = "fully_visible_in_review"
        elif any_source_present:
            relation_status = "peer_source_missing_this_run"
        else:
            relation_status = "peer_review_row_missing"

        relation_kind = next(
            (
                kind
                for kind in (
                    "card_payment",
                    "internal_budget",
                    "budget_boundary",
                    "transfer",
                )
                if any(member.get("_relation_kind") == kind for member in members)
            ),
            "transfer",
        )
        row_positions = sorted(
            {
                int(member["_row_pos"])
                for member in members
                if isinstance(member.get("_row_pos"), int)
            }
        )
        relation_seed = "|".join(
            [
                account_a,
                account_b,
                date,
                amount_text,
                ",".join(str(position) for position in row_positions),
            ]
        )
        relation_id = hashlib.sha1(relation_seed.encode("utf-8")).hexdigest()[:16]
        relation_rows.append(
            {
                "transfer_relation_id": relation_id,
                "relation_kind": relation_kind,
                "relation_status": relation_status,
                "date": date,
                "amount_abs_ils": float(amount_text),
                "account_a": account_a,
                "account_b": account_b,
                "row_positions": row_positions,
                "source_row_ids": sorted(
                    {
                        member["_source_row_id"]
                        for member in members
                        if member.get("_source_row_id")
                    }
                ),
                "target_row_ids": sorted(
                    {
                        member["_target_row_id"]
                        for member in members
                        if member.get("_target_row_id")
                    }
                ),
                "account_a_source_present": any(
                    bool(member.get("_source_present"))
                    for member in by_account.get(account_a, [])
                ),
                "account_a_target_present": any(
                    bool(member.get("_target_present"))
                    for member in by_account.get(account_a, [])
                ),
                "account_b_source_present": any(
                    bool(member.get("_source_present"))
                    for member in by_account.get(account_b, [])
                ),
                "account_b_target_present": any(
                    bool(member.get("_target_present"))
                    for member in by_account.get(account_b, [])
                ),
                "peer_review_row_present": peer_review_row_present,
                "ambiguous_relation": ambiguous_relation,
            }
        )

    if not relation_rows:
        return _empty_relation_frame()
    return pl.from_dicts(relation_rows, schema=_RELATION_SCHEMA, infer_schema_length=None)


def transfer_relation_lookup(relations: pl.DataFrame) -> dict[int, dict[str, Any]]:
    lookup: dict[int, dict[str, Any]] = {}
    if relations.is_empty():
        return lookup
    for relation in relations.to_dicts():
        row_positions = relation.get("row_positions") or []
        if not isinstance(row_positions, list):
            continue
        for row_pos in row_positions:
            if isinstance(row_pos, int):
                lookup[row_pos] = relation
    return lookup


def relation_allowed_decision_actions(
    df: pl.DataFrame,
    relation: dict[str, Any],
) -> list[str]:
    import ynab_il_importer.review_app.validation as review_validation

    row_positions = [
        row_pos
        for row_pos in relation.get("row_positions", [])
        if isinstance(row_pos, int) and 0 <= row_pos < len(df)
    ]
    if not row_positions:
        return [review_validation.NO_DECISION]
    allowed_lists = [
        review_validation.allowed_decision_actions(df.row(row_pos, named=True))
        for row_pos in row_positions
    ]
    ordered = allowed_lists[0][:]
    common = set(allowed_lists[0])
    for actions in allowed_lists[1:]:
        common &= set(actions)
    filtered = [action for action in ordered if action in common]
    return filtered or [review_validation.NO_DECISION]


def relation_default_decision(
    df: pl.DataFrame,
    relation: dict[str, Any],
) -> str:
    import ynab_il_importer.review_app.validation as review_validation

    row_positions = [
        row_pos
        for row_pos in relation.get("row_positions", [])
        if isinstance(row_pos, int) and 0 <= row_pos < len(df)
    ]
    actions = {
        review_validation.normalize_decision_action(
            df.row(row_pos, named=True).get("decision_action", review_validation.NO_DECISION)
        )
        for row_pos in row_positions
    }
    if len(actions) == 1:
        return next(iter(actions))
    return review_validation.NO_DECISION


def apply_transfer_relation(
    df: pl.DataFrame,
    relation: dict[str, Any],
    *,
    decision_action: str | None = None,
    reviewed: bool | None = None,
    component_map: dict[Any, int] | None = None,
) -> tuple[pl.DataFrame, list[int], list[str]]:
    import ynab_il_importer.review_app.model as review_model
    import ynab_il_importer.review_app.validation as review_validation

    if bool(relation.get("ambiguous_relation")):
        return df, [], ["Transfer relation is ambiguous; edit the member rows individually."]

    row_positions = [
        row_pos
        for row_pos in relation.get("row_positions", [])
        if isinstance(row_pos, int) and 0 <= row_pos < len(df)
    ]
    if not row_positions:
        return df, [], []

    updated = df
    affected = list(row_positions)
    if decision_action is not None:
        updated = review_model.apply_to_indices(
            updated,
            row_positions,
            decision_action=decision_action,
        )
        updated, competing_indices = review_model.apply_competing_row_resolution(
            updated,
            row_positions,
        )
        affected.extend(competing_indices)

    errors: list[str] = []
    if reviewed is not None:
        updated, errors = review_validation.apply_review_state(
            updated,
            affected,
            reviewed=reviewed,
            component_map=component_map,
        )
    return updated, list(dict.fromkeys(affected)), errors
