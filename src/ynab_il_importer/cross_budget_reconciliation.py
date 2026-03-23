from __future__ import annotations

from typing import Any

import pandas as pd


CLEARED_LIKE = {"cleared", "reconciled"}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _milliunits_to_ils(value: Any) -> float:
    try:
        return round(float(value) / 1000.0, 2)
    except (TypeError, ValueError):
        return 0.0


def _find_unique_named_category(
    category_groups: list[dict[str, Any]],
    *,
    category_name: str,
) -> dict[str, Any]:
    wanted = _normalize_text(category_name)
    matches: list[dict[str, Any]] = []
    for group in category_groups or []:
        for category in group.get("categories", []) or []:
            if bool(category.get("deleted", False)):
                continue
            if _normalize_text(category.get("name", "")) == wanted:
                row = dict(category)
                row["group_name"] = _normalize_text(group.get("name", ""))
                row["group_id"] = _normalize_text(group.get("id", ""))
                matches.append(row)
    if not matches:
        raise ValueError(f"Could not find YNAB category named {wanted!r}.")
    if len(matches) > 1:
        raise ValueError(f"Multiple YNAB categories share the name {wanted!r}.")
    return matches[0]


def _find_unique_named_account(
    accounts: list[dict[str, Any]],
    *,
    account_name: str,
) -> dict[str, Any]:
    wanted = _normalize_text(account_name)
    matches = [
        acc
        for acc in accounts or []
        if not bool(acc.get("deleted", False))
        and _normalize_text(acc.get("name", "")) == wanted
    ]
    if not matches:
        raise ValueError(f"Could not find YNAB account named {wanted!r}.")
    if len(matches) > 1:
        raise ValueError(f"Multiple YNAB accounts share the name {wanted!r}.")
    return matches[0]


def _status_bucket(value: Any) -> str:
    text = _normalize_text(value).lower()
    return text or "uncleared"


def _signed_amount_ils(df: pd.DataFrame) -> pd.Series:
    inflow = pd.to_numeric(df.get("inflow_ils", 0.0), errors="coerce").fillna(0.0)
    outflow = pd.to_numeric(df.get("outflow_ils", 0.0), errors="coerce").fillna(0.0)
    return (inflow - outflow).round(2)


def _target_account_transactions(
    transactions_df: pd.DataFrame,
    *,
    account_name: str,
) -> pd.DataFrame:
    if transactions_df.empty:
        return pd.DataFrame(columns=list(transactions_df.columns) + ["status_bucket", "signed_amount_ils"])
    wanted = _normalize_text(account_name)
    account_series = transactions_df.get(
        "account_name",
        pd.Series([""] * len(transactions_df), index=transactions_df.index),
    ).astype("string").fillna("").str.strip()
    out = transactions_df.loc[account_series == wanted].copy()
    if out.empty:
        out["status_bucket"] = pd.Series(dtype="string")
        out["signed_amount_ils"] = pd.Series(dtype="float64")
        return out
    out["status_bucket"] = out.get(
        "cleared",
        pd.Series([""] * len(out), index=out.index),
    ).map(_status_bucket)
    out["signed_amount_ils"] = _signed_amount_ils(out)
    return out


def _status_breakdown(target_txns: pd.DataFrame) -> pd.DataFrame:
    columns = ["status_bucket", "txn_count", "amount_ils"]
    if target_txns.empty:
        return pd.DataFrame(columns=columns)
    grouped = (
        target_txns.groupby("status_bucket", dropna=False)
        .agg(txn_count=("signed_amount_ils", "size"), amount_ils=("signed_amount_ils", "sum"))
        .reset_index()
    )
    grouped["amount_ils"] = grouped["amount_ils"].round(2)
    order = {"reconciled": 0, "cleared": 1, "uncleared": 2}
    grouped["_sort"] = grouped["status_bucket"].map(lambda v: order.get(str(v), 99))
    grouped = grouped.sort_values(["_sort", "status_bucket"]).drop(columns=["_sort"])
    return grouped[columns].reset_index(drop=True)


def build_cross_budget_balance_report(
    *,
    source_category_groups: list[dict[str, Any]],
    target_accounts: list[dict[str, Any]],
    target_transactions_df: pd.DataFrame,
    source_category_name: str,
    target_account_name: str,
    source_profile: str = "",
    target_profile: str = "",
) -> dict[str, pd.DataFrame]:
    category = _find_unique_named_category(
        source_category_groups,
        category_name=source_category_name,
    )
    account = _find_unique_named_account(
        target_accounts,
        account_name=target_account_name,
    )
    target_txns = _target_account_transactions(
        target_transactions_df,
        account_name=target_account_name,
    )
    breakdown = _status_breakdown(target_txns)

    reconciled_count = int((target_txns.get("status_bucket", "") == "reconciled").sum())
    cleared_only_count = int((target_txns.get("status_bucket", "") == "cleared").sum())
    uncleared_count = int((target_txns.get("status_bucket", "") == "uncleared").sum())
    cleared_like_mask = target_txns.get(
        "status_bucket",
        pd.Series([""] * len(target_txns), index=target_txns.index),
    ).isin(CLEARED_LIKE)
    cleared_txn_balance = round(float(target_txns.loc[cleared_like_mask, "signed_amount_ils"].sum()), 2)
    uncleared_txn_balance = round(float(target_txns.loc[~cleared_like_mask, "signed_amount_ils"].sum()), 2)
    total_txn_balance = round(float(target_txns.get("signed_amount_ils", pd.Series(dtype="float64")).sum()), 2)

    category_balance = _milliunits_to_ils(category.get("balance", 0))
    category_activity = _milliunits_to_ils(category.get("activity", 0))
    category_budgeted = _milliunits_to_ils(category.get("budgeted", 0))
    account_cleared_balance = _milliunits_to_ils(account.get("cleared_balance", 0))
    account_uncleared_balance = _milliunits_to_ils(account.get("uncleared_balance", 0))
    account_total_balance = _milliunits_to_ils(account.get("balance", 0))

    difference = round(category_balance - account_cleared_balance, 2)
    cleared_diff = round(cleared_txn_balance - account_cleared_balance, 2)
    uncleared_diff = round(uncleared_txn_balance - account_uncleared_balance, 2)
    total_diff = round(total_txn_balance - account_total_balance, 2)

    reconcile_actions = planned_reconciliation_actions(target_txns)
    summary = pd.DataFrame(
        [
            {
                "source_profile": _normalize_text(source_profile),
                "source_category": _normalize_text(source_category_name),
                "source_category_group": _normalize_text(category.get("group_name", "")),
                "source_category_balance_ils": category_balance,
                "source_category_activity_ils": category_activity,
                "source_category_budgeted_ils": category_budgeted,
                "target_profile": _normalize_text(target_profile),
                "target_account": _normalize_text(target_account_name),
                "target_account_cleared_balance_ils": account_cleared_balance,
                "target_account_uncleared_balance_ils": account_uncleared_balance,
                "target_account_total_balance_ils": account_total_balance,
                "target_txn_reconciled_count": reconciled_count,
                "target_txn_cleared_count": cleared_only_count,
                "target_txn_uncleared_count": uncleared_count,
                "target_txn_total_count": len(target_txns),
                "target_txn_cleared_balance_ils": cleared_txn_balance,
                "target_txn_uncleared_balance_ils": uncleared_txn_balance,
                "target_txn_total_balance_ils": total_txn_balance,
                "updates_planned": len(reconcile_actions),
                "difference_ils": difference,
                "target_cleared_balance_diff_ils": cleared_diff,
                "target_uncleared_balance_diff_ils": uncleared_diff,
                "target_total_balance_diff_ils": total_diff,
                "is_balanced": abs(difference) < 0.005,
            }
        ]
    )
    return {
        "summary": summary,
        "status_breakdown": breakdown,
        "target_transactions": target_txns,
        "target_report": build_target_reconciliation_report(target_txns),
    }


def reconciliation_issues(summary_df: pd.DataFrame, *, tolerance: float = 0.005) -> list[str]:
    if summary_df.empty:
        return ["Missing reconciliation summary row."]
    row = summary_df.iloc[0]
    issues: list[str] = []
    difference = float(pd.to_numeric(row.get("difference_ils", 0.0), errors="coerce") or 0.0)
    if abs(difference) >= tolerance:
        issues.append(
            "Family category balance does not equal the Pilates cleared account balance "
            f"({difference:.2f} ILS difference)."
        )
    cleared_diff = float(
        pd.to_numeric(row.get("target_cleared_balance_diff_ils", 0.0), errors="coerce") or 0.0
    )
    if abs(cleared_diff) >= tolerance:
        issues.append(
            "Target transaction cleared-sum does not match the Pilates account cleared balance "
            f"({cleared_diff:.2f} ILS difference)."
        )
    total_diff = float(
        pd.to_numeric(row.get("target_total_balance_diff_ils", 0.0), errors="coerce") or 0.0
    )
    if abs(total_diff) >= tolerance:
        issues.append(
            "Target transaction total-sum does not match the Pilates account total balance "
            f"({total_diff:.2f} ILS difference)."
        )
    return issues


def build_target_reconciliation_report(target_txns: pd.DataFrame) -> pd.DataFrame:
    if target_txns.empty:
        return pd.DataFrame(
            columns=[
                "ynab_id",
                "date",
                "account_name",
                "payee_raw",
                "category_raw",
                "memo",
                "outflow_ils",
                "inflow_ils",
                "signed_amount_ils",
                "cleared",
                "status_bucket",
                "action",
            ]
        )
    out = target_txns.copy()
    status = out.get(
        "status_bucket",
        pd.Series([""] * len(out), index=out.index),
    ).astype("string").fillna("")
    out["action"] = "leave_uncleared"
    out.loc[status == "cleared", "action"] = "reconcile"
    out.loc[status == "reconciled", "action"] = "already_reconciled"
    columns = [
        "ynab_id",
        "date",
        "account_name",
        "payee_raw",
        "category_raw",
        "memo",
        "outflow_ils",
        "inflow_ils",
        "signed_amount_ils",
        "cleared",
        "status_bucket",
        "action",
    ]
    return out[columns].copy()


def planned_reconciliation_actions(target_txns: pd.DataFrame) -> list[dict[str, str]]:
    if target_txns.empty:
        return []
    report = build_target_reconciliation_report(target_txns)
    reconcile_rows = report[report["action"] == "reconcile"].copy()
    updates: list[dict[str, str]] = []
    for _, row in reconcile_rows.iterrows():
        txn_id = _normalize_text(row.get("ynab_id", ""))
        if not txn_id:
            continue
        updates.append({"id": txn_id, "cleared": "reconciled"})
    return updates
