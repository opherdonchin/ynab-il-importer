from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import polars as pl


CATEGORY_ACCOUNT_RECONCILE_REPORT_COLUMNS = [
    "review_transaction_id",
    "decision_action",
    "source_date",
    "source_payee",
    "source_amount_ils",
    "target_account_name",
    "expected_import_id",
    "expected_existing_transaction_id",
    "resolved_transaction_id",
    "resolved_import_id",
    "resolved_payee_name",
    "resolved_date",
    "resolved_amount_ils",
    "prior_cleared",
    "action",
    "reason",
]
_RECONCILE_DECISIONS = {"keep_match", "create_target", "update_target"}


@dataclass(frozen=True)
class CategoryReconcileSource:
    category_id: str
    category_name: str
    target_account_id: str
    target_account_name: str


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _same_amount(left: float, right: float) -> bool:
    return abs(round(float(left), 2) - round(float(right), 2)) <= 0.005


def run_month_from_tag(run_tag: str) -> str:
    text = _normalize_text(run_tag)
    if not text:
        raise ValueError("run_tag cannot be empty.")
    try:
        parsed = datetime.strptime(text, "%Y_%m_%d")
    except ValueError as exc:
        raise ValueError(
            f"run_tag must look like YYYY_MM_DD for ynab_category reconciliation, got {text!r}."
        ) from exc
    return parsed.strftime("%Y-%m-01")


def select_review_rows_for_source(
    reviewed_df: pl.DataFrame,
    *,
    source: CategoryReconcileSource,
) -> pl.DataFrame:
    if reviewed_df.is_empty():
        return reviewed_df.clone()

    relevant_context_kinds = ["ynab_parent_category_match", "ynab_split_category_match"]
    category_id = _normalize_text(source.category_id)
    category_name = _normalize_text(source.category_name)
    target_account_name = _normalize_text(source.target_account_name)

    filtered = reviewed_df.filter(
        pl.col("source_present").cast(pl.Boolean, strict=False).fill_null(False)
        & pl.col("reviewed").cast(pl.Boolean, strict=False).fill_null(False)
        & pl.col("source_context_kind")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .is_in(relevant_context_kinds)
        & (
            (
                pl.lit(category_id != "")
                & pl.col("source_context_category_id")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                .eq(category_id)
            )
            | (
                pl.lit(category_id == "")
                & pl.col("source_context_category_name")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                .eq(category_name)
            )
        )
        & pl.col("target_account")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .eq(target_account_name)
    )
    if filtered.is_empty():
        return filtered
    return filtered.sort(
        ["source_date", "source_payee_current", "transaction_id"], nulls_last=True
    )


def resolve_month_category(
    month_detail: dict[str, Any],
    *,
    category_id: str = "",
    category_name: str = "",
) -> dict[str, Any]:
    wanted_id = _normalize_text(category_id)
    wanted_name = _normalize_text(category_name)
    categories = month_detail.get("categories", []) or []
    matches = []
    for category in categories:
        if bool(category.get("deleted", False)):
            continue
        if wanted_id and _normalize_text(category.get("id", "")) == wanted_id:
            matches.append(category)
            continue
        if wanted_name and _normalize_text(category.get("name", "")) == wanted_name:
            matches.append(category)
    if not matches:
        raise ValueError(
            f"Could not resolve live source category id={wanted_id!r} name={wanted_name!r} in month detail."
        )
    if len(matches) != 1:
        raise ValueError(
            f"Source category resolution is ambiguous for id={wanted_id!r} name={wanted_name!r}: "
            f"{[_normalize_text(match.get('name', '')) for match in matches]}"
        )
    return matches[0]


def resolve_live_account(
    accounts: list[dict[str, Any]],
    *,
    account_id: str = "",
    account_name: str = "",
) -> dict[str, Any]:
    wanted_id = _normalize_text(account_id)
    wanted_name = _normalize_text(account_name)
    matches = []
    for account in accounts:
        if bool(account.get("deleted", False)):
            continue
        if wanted_id and _normalize_text(account.get("id", "")) == wanted_id:
            matches.append(account)
            continue
        if wanted_name and _normalize_text(account.get("name", "")) == wanted_name:
            matches.append(account)
    if not matches:
        raise ValueError(
            f"Could not resolve live target account id={wanted_id!r} name={wanted_name!r}."
        )
    if len(matches) != 1:
        raise ValueError(
            f"Target account resolution is ambiguous for id={wanted_id!r} name={wanted_name!r}: "
            f"{[_normalize_text(match.get('name', '')) for match in matches]}"
        )
    return matches[0]


def _resolved_amount_ils(txn: dict[str, Any]) -> float:
    return round(float(txn.get("amount", 0) or 0) / 1000.0, 2)


def _source_amount_ils(row: dict[str, Any]) -> float:
    return round(
        float(row.get("inflow_ils", 0.0) or 0.0)
        - float(row.get("outflow_ils", 0.0) or 0.0),
        2,
    )


def _report_row(row: dict[str, Any], target_account: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_transaction_id": _normalize_text(row.get("transaction_id", "")),
        "decision_action": _normalize_text(row.get("decision_action", "")),
        "source_date": _normalize_text(row.get("source_date", "")),
        "source_payee": _normalize_text(row.get("source_payee_current", "")),
        "source_amount_ils": _source_amount_ils(row),
        "target_account_name": _normalize_text(target_account.get("name", "")),
        "expected_import_id": "",
        "expected_existing_transaction_id": "",
        "resolved_transaction_id": "",
        "resolved_import_id": "",
        "resolved_payee_name": "",
        "resolved_date": "",
        "resolved_amount_ils": 0.0,
        "prior_cleared": "",
        "action": "",
        "reason": "",
    }


def plan_category_account_reconciliation(
    reviewed_rows: pl.DataFrame,
    prepared_units: pl.DataFrame,
    *,
    target_transactions: list[dict[str, Any]],
    target_account: dict[str, Any],
    source_category: dict[str, Any],
) -> dict[str, Any]:
    relevant = reviewed_rows.clone()
    units = prepared_units.clone()

    transactions_by_id = {
        _normalize_text(txn.get("id", "")): txn
        for txn in target_transactions
        if _normalize_text(txn.get("id", ""))
    }
    transactions_by_account_import = {
        (
            _normalize_text(txn.get("account_id", "")),
            _normalize_text(txn.get("import_id", "")),
        ): txn
        for txn in target_transactions
        if _normalize_text(txn.get("account_id", ""))
        and _normalize_text(txn.get("import_id", ""))
    }
    units_by_review_transaction_id = {
        _normalize_text(row.get("upload_transaction_id", "")): row
        for row in units.to_dicts()
        if _normalize_text(row.get("upload_transaction_id", ""))
    }

    report_rows: list[dict[str, Any]] = []
    update_ids: set[str] = set()
    updates: list[dict[str, str]] = []
    blocked_reason = ""

    for row in relevant.to_dicts():
        review_transaction_id = _normalize_text(row.get("transaction_id", ""))
        decision_action = _normalize_text(row.get("decision_action", ""))
        report_row = _report_row(row, target_account)
        resolved_txn: dict[str, Any] | None = None

        if decision_action == "keep_match":
            existing_id = _normalize_text(
                row.get(
                    "target_row_id",
                    row.get("target_transaction_id", row.get("target_ynab_id", "")),
                )
            )
            report_row["expected_existing_transaction_id"] = existing_id
            resolved_txn = transactions_by_id.get(existing_id)
            if resolved_txn is None:
                report_row["action"] = "blocked"
                report_row["reason"] = "missing_target_transaction_id_in_live_ynab"
        elif decision_action in {"create_target", "update_target"}:
            unit = units_by_review_transaction_id.get(review_transaction_id)
            if unit is None:
                report_row["action"] = "blocked"
                report_row["reason"] = "missing_prepared_upload_unit"
            else:
                expected_import_id = _normalize_text(unit.get("import_id", ""))
                existing_id = _normalize_text(unit.get("existing_transaction_id", ""))
                account_id = _normalize_text(unit.get("account_id", ""))
                report_row["expected_import_id"] = expected_import_id
                report_row["expected_existing_transaction_id"] = existing_id
                if decision_action == "create_target":
                    if not expected_import_id:
                        report_row["action"] = "blocked"
                        report_row["reason"] = "missing_prepared_upload_import_id"
                    else:
                        resolved_txn = transactions_by_account_import.get(
                            (account_id, expected_import_id)
                        )
                        if resolved_txn is None:
                            report_row["action"] = "blocked"
                            report_row["reason"] = (
                                "missing_uploaded_transaction_in_live_ynab"
                            )
                else:
                    if not existing_id:
                        report_row["action"] = "blocked"
                        report_row["reason"] = (
                            "missing_prepared_existing_transaction_id"
                        )
                    else:
                        resolved_txn = transactions_by_id.get(existing_id)
                        if resolved_txn is None:
                            report_row["action"] = "blocked"
                            report_row["reason"] = (
                                "missing_update_target_transaction_in_live_ynab"
                            )
        elif decision_action in _RECONCILE_DECISIONS:
            report_row["action"] = "blocked"
            report_row["reason"] = "unsupported_reconcile_decision"
        else:
            report_row["action"] = "skipped"
            report_row["reason"] = "decision_action_not_reconcilable"

        if resolved_txn is not None:
            resolved_amount_ils = _resolved_amount_ils(resolved_txn)
            resolved_transaction_id = _normalize_text(resolved_txn.get("id", ""))
            report_row["resolved_transaction_id"] = resolved_transaction_id
            report_row["resolved_import_id"] = _normalize_text(
                resolved_txn.get("import_id", "")
            )
            report_row["resolved_payee_name"] = _normalize_text(
                resolved_txn.get("payee_name", "")
            )
            report_row["resolved_date"] = _normalize_text(resolved_txn.get("date", ""))
            report_row["resolved_amount_ils"] = resolved_amount_ils
            report_row["prior_cleared"] = _normalize_text(resolved_txn.get("cleared", ""))
            if not _same_amount(resolved_amount_ils, report_row["source_amount_ils"]):
                report_row["action"] = "blocked"
                report_row["reason"] = "resolved_amount_mismatch"
            elif report_row["prior_cleared"] == "reconciled":
                report_row["action"] = "already_reconciled"
                report_row["reason"] = ""
            else:
                report_row["action"] = "reconcile"
                report_row["reason"] = ""
                if resolved_transaction_id and resolved_transaction_id not in update_ids:
                    update_ids.add(resolved_transaction_id)
                    updates.append(
                        {
                            "id": resolved_transaction_id,
                            "cleared": "reconciled",
                        }
                    )

        report_rows.append(report_row)
        if report_row["action"] == "blocked" and not blocked_reason:
            blocked_reason = (
                f"{review_transaction_id or '<unknown>'}: {report_row['reason'] or 'blocked'}"
            )

    source_category_balance_ils = round(
        float(source_category.get("balance", 0) or 0) / 1000.0,
        2,
    )
    target_account_balance_ils = round(
        float(target_account.get("balance", 0) or 0) / 1000.0,
        2,
    )
    target_account_cleared_balance_ils = round(
        float(target_account.get("cleared_balance", 0) or 0) / 1000.0,
        2,
    )
    target_account_uncleared_balance_ils = round(
        float(target_account.get("uncleared_balance", 0) or 0) / 1000.0,
        2,
    )
    balance_parity_ok = _same_amount(
        source_category_balance_ils, target_account_balance_ils
    )
    cleared_parity_ok = _same_amount(
        source_category_balance_ils, target_account_cleared_balance_ils
    )
    uncleared_zero_ok = _same_amount(target_account_uncleared_balance_ils, 0.0)

    if not blocked_reason and not balance_parity_ok:
        blocked_reason = (
            f"category/account balance mismatch: source {source_category_balance_ils:.2f} "
            f"vs target balance {target_account_balance_ils:.2f}"
        )
    if not blocked_reason and not cleared_parity_ok:
        blocked_reason = (
            f"category/cleared balance mismatch: source {source_category_balance_ils:.2f} "
            f"vs target cleared {target_account_cleared_balance_ils:.2f}"
        )
    if not blocked_reason and not uncleared_zero_ok:
        blocked_reason = (
            f"target account still has uncleared balance {target_account_uncleared_balance_ils:.2f}"
        )

    report = pd.DataFrame(
        report_rows,
        columns=CATEGORY_ACCOUNT_RECONCILE_REPORT_COLUMNS,
    )
    action_counts = (
        report["action"].astype("string").fillna("").value_counts().to_dict()
        if not report.empty
        else {}
    )
    return {
        "ok": not bool(blocked_reason),
        "reason": blocked_reason,
        "report": report,
        "updates": updates,
        "reviewed_row_count": len(report_rows),
        "resolved_count": int((report["resolved_transaction_id"] != "").sum())
        if not report.empty
        else 0,
        "reconcile_count": int(action_counts.get("reconcile", 0)),
        "already_reconciled_count": int(action_counts.get("already_reconciled", 0)),
        "skipped_count": int(action_counts.get("skipped", 0)),
        "blocked_count": int(action_counts.get("blocked", 0)),
        "update_count": len(updates),
        "source_category_name": _normalize_text(source_category.get("name", "")),
        "source_category_id": _normalize_text(source_category.get("id", "")),
        "source_category_balance_ils": source_category_balance_ils,
        "target_account_name": _normalize_text(target_account.get("name", "")),
        "target_account_id": _normalize_text(target_account.get("id", "")),
        "target_account_balance_ils": target_account_balance_ils,
        "target_account_cleared_balance_ils": target_account_cleared_balance_ils,
        "target_account_uncleared_balance_ils": target_account_uncleared_balance_ils,
        "balance_parity_ok": balance_parity_ok,
        "cleared_parity_ok": cleared_parity_ok,
        "uncleared_zero_ok": uncleared_zero_ok,
    }
