from __future__ import annotations

from typing import Any

import pandas as pd

import ynab_il_importer.cross_budget_pairing as cross_budget_pairing


CLEARED_LIKE = {"cleared", "reconciled"}
MONTH_REPORT_COLUMNS = [
    "month",
    "month_end",
    "source_category_group",
    "source_category_balance_ils",
    "source_category_activity_ils",
    "source_category_budgeted_ils",
    "target_cleared_balance_ils",
    "target_total_balance_ils",
    "target_reconciled_count",
    "target_cleared_count",
    "target_uncleared_count",
    "target_cleared_activity_ils",
    "source_balance_change_ils",
    "target_cleared_balance_change_ils",
    "difference_change_ils",
    "source_other_balance_change_ils",
    "target_other_balance_change_ils",
    "difference_ils",
    "is_exact_balance_anchor",
]
SOURCE_MONTH_REPORT_COLUMNS = [
    "month",
    "month_end",
    "source_category_group",
    "source_category_balance_ils",
    "source_category_activity_ils",
    "source_category_budgeted_ils",
]
SOURCE_RECONCILE_REPORT_COLUMNS = [
    "source_row_id",
    "date",
    "source_account",
    "payee_raw",
    "category_raw",
    "memo",
    "outflow_ils",
    "inflow_ils",
    "signed_amount_ils",
    "match_status",
    "target_row_id",
    "ynab_id",
    "ynab_payee_raw",
    "ynab_category_raw",
    "ynab_cleared",
    "action",
    "reason",
]


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


def _to_timestamp(value: Any) -> pd.Timestamp | pd.NaT:
    parsed = pd.to_datetime(value, errors="coerce")
    return parsed.normalize() if not pd.isna(parsed) else pd.NaT


def _parse_optional_date(value: Any) -> pd.Timestamp | None:
    text = _normalize_text(value)
    if not text:
        return None
    parsed = _to_timestamp(text)
    if pd.isna(parsed):
        raise ValueError(f"Invalid date value {value!r}; expected YYYY-MM-DD.")
    return parsed


def _report_ids_from_ambiguous(df: pd.DataFrame, *, singular: str, plural: str) -> set[str]:
    ids: set[str] = set()
    if df.empty:
        return ids
    if singular in df.columns:
        series = df[singular].astype("string").fillna("")
        ids.update(value.strip() for value in series.tolist() if value and value.strip())
    if plural in df.columns:
        series = df[plural].astype("string").fillna("")
        for value in series.tolist():
            parts = [part.strip() for part in str(value).split(";")]
            ids.update(part for part in parts if part)
    return ids


def _build_source_reconcile_report(
    *,
    source_df: pd.DataFrame,
    match_result: cross_budget_pairing.CrossBudgetMatchResult,
) -> pd.DataFrame:
    if source_df.empty:
        return pd.DataFrame(columns=SOURCE_RECONCILE_REPORT_COLUMNS)

    matched = match_result.matched_pairs_df.copy()
    matched_by_source: dict[str, dict[str, Any]] = {}
    if not matched.empty:
        for _, row in matched.iterrows():
            matched_by_source[_normalize_text(row.get("source_row_id", ""))] = row.to_dict()

    ambiguous_source_ids = _report_ids_from_ambiguous(
        match_result.ambiguous_matches_df,
        singular="source_row_id",
        plural="source_row_ids",
    )

    ordered = source_df.copy().sort_values(["date_key", "source_row_id"], kind="stable")
    rows: list[dict[str, Any]] = []
    for _, row in ordered.iterrows():
        source_row_id = _normalize_text(row.get("source_row_id", ""))
        item = {
            "source_row_id": source_row_id,
            "date": _normalize_text(row.get("date", "")),
            "source_account": _normalize_text(row.get("source_account", "")),
            "payee_raw": _normalize_text(row.get("payee_raw", "")),
            "category_raw": _normalize_text(row.get("category_raw", "")),
            "memo": _normalize_text(row.get("memo", "")),
            "outflow_ils": round(float(pd.to_numeric(row.get("outflow_ils", 0.0), errors="coerce") or 0.0), 2),
            "inflow_ils": round(float(pd.to_numeric(row.get("inflow_ils", 0.0), errors="coerce") or 0.0), 2),
            "signed_amount_ils": round(float(pd.to_numeric(row.get("signed_amount", 0.0), errors="coerce") or 0.0), 2),
            "match_status": "unmatched_source",
            "target_row_id": "",
            "ynab_id": "",
            "ynab_payee_raw": "",
            "ynab_category_raw": "",
            "ynab_cleared": "",
            "action": "",
            "reason": "",
        }
        if source_row_id in matched_by_source:
            pair = matched_by_source[source_row_id]
            item.update(
                {
                    "match_status": "matched_existing",
                    "target_row_id": _normalize_text(pair.get("target_row_id", "")),
                    "ynab_id": _normalize_text(pair.get("ynab_id", "")),
                    "ynab_payee_raw": _normalize_text(pair.get("ynab_payee_raw", "")),
                    "ynab_category_raw": _normalize_text(pair.get("ynab_category_raw", "")),
                    "ynab_cleared": _normalize_text(pair.get("ynab_cleared", "")),
                }
            )
        elif source_row_id in ambiguous_source_ids:
            item["match_status"] = "ambiguous"
            item["reason"] = "ambiguous cross-budget match candidates"
        else:
            item["reason"] = "no matched target row"
        rows.append(item)
    return pd.DataFrame(rows, columns=SOURCE_RECONCILE_REPORT_COLUMNS)


def _build_cross_budget_target_report(
    *,
    target_df: pd.DataFrame,
    match_result: cross_budget_pairing.CrossBudgetMatchResult,
) -> pd.DataFrame:
    columns = [
        "target_row_id",
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
        "match_status",
        "source_row_id",
        "action",
        "reason",
    ]
    if target_df.empty:
        return pd.DataFrame(columns=columns)

    matched = match_result.matched_pairs_df.copy()
    matched_by_target: dict[str, dict[str, Any]] = {}
    if not matched.empty:
        for _, row in matched.iterrows():
            matched_by_target[_normalize_text(row.get("target_row_id", ""))] = row.to_dict()

    ambiguous_target_ids = _report_ids_from_ambiguous(
        match_result.ambiguous_matches_df,
        singular="target_row_id",
        plural="target_row_ids",
    )

    ordered = target_df.copy().sort_values(["date_key", "target_row_id"], kind="stable")
    rows: list[dict[str, Any]] = []
    for _, row in ordered.iterrows():
        target_row_id = _normalize_text(row.get("target_row_id", ""))
        item = {
            "target_row_id": target_row_id,
            "ynab_id": _normalize_text(row.get("ynab_id", "")),
            "date": _normalize_text(row.get("date", "")),
            "account_name": _normalize_text(row.get("account_name", "")),
            "payee_raw": _normalize_text(row.get("payee_raw", "")),
            "category_raw": _normalize_text(row.get("category_raw", "")),
            "memo": _normalize_text(row.get("memo", "")),
            "outflow_ils": round(float(pd.to_numeric(row.get("outflow_ils", 0.0), errors="coerce") or 0.0), 2),
            "inflow_ils": round(float(pd.to_numeric(row.get("inflow_ils", 0.0), errors="coerce") or 0.0), 2),
            "signed_amount_ils": round(float(pd.to_numeric(row.get("signed_amount", 0.0), errors="coerce") or 0.0), 2),
            "cleared": _normalize_text(row.get("cleared", "")),
            "match_status": "unmatched_target",
            "source_row_id": "",
            "action": "",
            "reason": "",
        }
        if target_row_id in matched_by_target:
            pair = matched_by_target[target_row_id]
            item.update(
                {
                    "match_status": "matched_existing",
                    "source_row_id": _normalize_text(pair.get("source_row_id", "")),
                }
            )
        elif target_row_id in ambiguous_target_ids:
            item["match_status"] = "ambiguous"
            item["reason"] = "ambiguous cross-budget match candidates"
        else:
            item["reason"] = "no matched Family-category row"
        rows.append(item)
    return pd.DataFrame(rows, columns=columns)


def _extract_month_category(
    month_detail: dict[str, Any],
    *,
    category_name: str,
) -> dict[str, Any] | None:
    wanted = _normalize_text(category_name)
    for category in month_detail.get("categories", []) or []:
        if bool(category.get("deleted", False)):
            continue
        if _normalize_text(category.get("name", "")) == wanted:
            return category
    return None


def _source_month_report_from_details(
    *,
    source_month_details: list[dict[str, Any]],
    source_category_name: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for detail in source_month_details or []:
        month_value = _normalize_text(detail.get("month", ""))
        if not month_value:
            continue
        category = _extract_month_category(detail, category_name=source_category_name)
        if category is None:
            continue
        month_end = _to_timestamp(month_value) + pd.offsets.MonthEnd(0)
        rows.append(
            {
                "month": month_value,
                "month_end": month_end.strftime("%Y-%m-%d"),
                "source_category_group": _normalize_text(category.get("category_group_name", "")),
                "source_category_balance_ils": _milliunits_to_ils(category.get("balance", 0)),
                "source_category_activity_ils": _milliunits_to_ils(category.get("activity", 0)),
                "source_category_budgeted_ils": _milliunits_to_ils(category.get("budgeted", 0)),
            }
        )
    if not rows:
        return pd.DataFrame(columns=SOURCE_MONTH_REPORT_COLUMNS)
    return pd.DataFrame(rows, columns=SOURCE_MONTH_REPORT_COLUMNS)


def _validate_source_month_report(source_month_report_df: pd.DataFrame) -> pd.DataFrame:
    if source_month_report_df is None or source_month_report_df.empty:
        return pd.DataFrame(columns=SOURCE_MONTH_REPORT_COLUMNS)
    missing = sorted(set(SOURCE_MONTH_REPORT_COLUMNS) - set(source_month_report_df.columns))
    if missing:
        raise ValueError(
            "Cached source month report is missing required columns: "
            f"{missing}"
        )
    out = source_month_report_df.copy()
    for col in ["source_category_balance_ils", "source_category_activity_ils", "source_category_budgeted_ils"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).round(2)
    out["month"] = out["month"].map(_normalize_text)
    out["month_end"] = out["month_end"].map(_normalize_text)
    out["source_category_group"] = out["source_category_group"].map(_normalize_text)
    return out[SOURCE_MONTH_REPORT_COLUMNS].copy()


def build_cross_budget_month_report(
    *,
    source_month_details: list[dict[str, Any]],
    target_transactions_df: pd.DataFrame,
    source_category_name: str,
    target_account_name: str,
    tolerance: float = 0.005,
) -> pd.DataFrame:
    source_month_report = _source_month_report_from_details(
        source_month_details=source_month_details,
        source_category_name=source_category_name,
    )
    return build_cross_budget_month_report_from_source_history(
        source_month_report_df=source_month_report,
        target_transactions_df=target_transactions_df,
        target_account_name=target_account_name,
        tolerance=tolerance,
    )


def build_cross_budget_month_report_from_source_history(
    *,
    source_month_report_df: pd.DataFrame,
    target_transactions_df: pd.DataFrame,
    target_account_name: str,
    tolerance: float = 0.005,
) -> pd.DataFrame:
    source_month_report = _validate_source_month_report(source_month_report_df)
    target_txns = _target_account_transactions(
        target_transactions_df,
        account_name=target_account_name,
    ).copy()
    if not target_txns.empty:
        target_txns["date_ts"] = pd.to_datetime(target_txns["date"], errors="coerce")

    rows: list[dict[str, Any]] = []
    for _, detail in source_month_report.iterrows():
        month_value = _normalize_text(detail.get("month", ""))
        if not month_value:
            continue
        month_end = _to_timestamp(detail.get("month_end", "")) or (_to_timestamp(month_value) + pd.offsets.MonthEnd(0))
        upto = (
            target_txns.loc[target_txns["date_ts"] <= month_end].copy()
            if not target_txns.empty
            else target_txns.copy()
        )
        within_month = (
            target_txns.loc[
                (target_txns["date_ts"] >= _to_timestamp(month_value))
                & (target_txns["date_ts"] <= month_end)
            ].copy()
            if not target_txns.empty
            else target_txns.copy()
        )
        cleared_like_mask = upto.get(
            "status_bucket",
            pd.Series([""] * len(upto), index=upto.index),
        ).isin(CLEARED_LIKE)
        month_cleared_like_mask = within_month.get(
            "status_bucket",
            pd.Series([""] * len(within_month), index=within_month.index),
        ).isin(CLEARED_LIKE)
        rows.append(
            {
                "month": month_value,
                "month_end": month_end.strftime("%Y-%m-%d"),
                "source_category_group": _normalize_text(detail.get("source_category_group", "")),
                "source_category_balance_ils": round(float(detail.get("source_category_balance_ils", 0.0) or 0.0), 2),
                "source_category_activity_ils": round(float(detail.get("source_category_activity_ils", 0.0) or 0.0), 2),
                "source_category_budgeted_ils": round(float(detail.get("source_category_budgeted_ils", 0.0) or 0.0), 2),
                "target_cleared_balance_ils": round(float(upto.loc[cleared_like_mask, "signed_amount_ils"].sum()), 2)
                if not upto.empty
                else 0.0,
                "target_total_balance_ils": round(float(upto.get("signed_amount_ils", pd.Series(dtype="float64")).sum()), 2)
                if not upto.empty
                else 0.0,
                "target_reconciled_count": int((upto.get("status_bucket", "") == "reconciled").sum()) if not upto.empty else 0,
                "target_cleared_count": int((upto.get("status_bucket", "") == "cleared").sum()) if not upto.empty else 0,
                "target_uncleared_count": int((upto.get("status_bucket", "") == "uncleared").sum()) if not upto.empty else 0,
                "target_cleared_activity_ils": round(float(within_month.loc[month_cleared_like_mask, "signed_amount_ils"].sum()), 2)
                if not within_month.empty
                else 0.0,
            }
        )
    report = pd.DataFrame(rows)
    if report.empty:
        return pd.DataFrame(columns=MONTH_REPORT_COLUMNS)
    report["source_balance_change_ils"] = report["source_category_balance_ils"].diff().fillna(report["source_category_balance_ils"]).round(2)
    report["target_cleared_balance_change_ils"] = report["target_cleared_balance_ils"].diff().fillna(report["target_cleared_balance_ils"]).round(2)
    report["difference_change_ils"] = (
        report["source_balance_change_ils"] - report["target_cleared_balance_change_ils"]
    ).round(2)
    report["source_other_balance_change_ils"] = (
        report["source_balance_change_ils"] - report["source_category_activity_ils"]
    ).round(2)
    report["target_other_balance_change_ils"] = (
        report["target_cleared_balance_change_ils"] - report["target_cleared_activity_ils"]
    ).round(2)
    report["difference_ils"] = (
        report["source_category_balance_ils"] - report["target_cleared_balance_ils"]
    ).round(2)
    report["is_exact_balance_anchor"] = report["difference_ils"].abs() < tolerance
    return report[MONTH_REPORT_COLUMNS].copy()


def _rows_all_reconciled_match(df: pd.DataFrame) -> bool:
    if df.empty:
        return True
    return bool(
        df["match_status"].astype("string").fillna("").eq("matched_existing").all()
        and df["ynab_cleared"].astype("string").fillna("").eq("reconciled").all()
    )


def _target_rows_all_reconciled_match(df: pd.DataFrame) -> bool:
    if df.empty:
        return True
    return bool(
        df["match_status"].astype("string").fillna("").eq("matched_existing").all()
        and df["cleared"].astype("string").fillna("").eq("reconciled").all()
    )


def _first_problem_row(df: pd.DataFrame, *, date_col: str, description_col: str) -> str:
    if df.empty:
        return ""
    row = df.iloc[0]
    return (
        f"{_normalize_text(row.get(date_col, ''))} "
        f"{_normalize_text(row.get(description_col, ''))} "
        f"status={_normalize_text(row.get('match_status', '')) or _normalize_text(row.get('action', ''))}"
    ).strip()


def _mark_actions(
    *,
    source_report: pd.DataFrame,
    target_report: pd.DataFrame,
    anchor_source_ids: set[str],
    anchor_target_ids: set[str],
    pre_window_source_ids: set[str],
    pre_window_target_ids: set[str],
    active_source_ids: set[str],
    active_target_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    source_out = source_report.copy()
    target_out = target_report.copy()

    source_out["action"] = ""
    target_out["action"] = ""
    source_out.loc[source_out["source_row_id"].isin(pre_window_source_ids), "action"] = "pre_window_history"
    target_out.loc[target_out["target_row_id"].isin(pre_window_target_ids), "action"] = "pre_window_history"
    source_out.loc[source_out["source_row_id"].isin(anchor_source_ids), "action"] = "anchor_history"
    target_out.loc[target_out["target_row_id"].isin(anchor_target_ids), "action"] = "anchor_history"

    active_source = source_out["source_row_id"].isin(active_source_ids)
    active_target = target_out["target_row_id"].isin(active_target_ids)

    source_out.loc[
        active_source
        & source_out["match_status"].astype("string").fillna("").eq("matched_existing")
        & source_out["ynab_cleared"].astype("string").fillna("").eq("reconciled"),
        "action",
    ] = "already_reconciled"
    source_out.loc[
        active_source
        & source_out["match_status"].astype("string").fillna("").eq("matched_existing")
        & ~source_out["ynab_cleared"].astype("string").fillna("").eq("reconciled"),
        "action",
    ] = "reconcile"
    source_out.loc[
        active_source & source_out["match_status"].astype("string").fillna("").eq("unmatched_source"),
        "action",
    ] = "blocked_unmatched"
    source_out.loc[
        active_source & source_out["match_status"].astype("string").fillna("").eq("ambiguous"),
        "action",
    ] = "blocked_ambiguous"

    target_out.loc[
        active_target
        & target_out["match_status"].astype("string").fillna("").eq("matched_existing")
        & target_out["cleared"].astype("string").fillna("").eq("reconciled"),
        "action",
    ] = "already_reconciled"
    target_out.loc[
        active_target
        & target_out["match_status"].astype("string").fillna("").eq("matched_existing")
        & ~target_out["cleared"].astype("string").fillna("").eq("reconciled"),
        "action",
    ] = "reconcile"
    target_out.loc[
        active_target & target_out["match_status"].astype("string").fillna("").eq("unmatched_target"),
        "action",
    ] = "blocked_unmatched"
    target_out.loc[
        active_target & target_out["match_status"].astype("string").fillna("").eq("ambiguous"),
        "action",
    ] = "blocked_ambiguous"
    return source_out, target_out


def plan_cross_budget_reconciliation(
    *,
    source_category_groups: list[dict[str, Any]],
    source_month_details: list[dict[str, Any]],
    source_month_report_df: pd.DataFrame | None = None,
    target_accounts: list[dict[str, Any]],
    source_transactions_df: pd.DataFrame,
    target_transactions_df: pd.DataFrame,
    source_category_name: str,
    target_account_name: str,
    since: str | None = None,
    anchor_streak: int = 7,
    date_tolerance_days: int = 0,
    source_profile: str = "",
    target_profile: str = "",
) -> dict[str, Any]:
    if anchor_streak < 1:
        raise ValueError("anchor_streak must be at least 1.")

    current = build_cross_budget_balance_report(
        source_category_groups=source_category_groups,
        target_accounts=target_accounts,
        target_transactions_df=target_transactions_df,
        source_category_name=source_category_name,
        target_account_name=target_account_name,
        source_profile=source_profile,
        target_profile=target_profile,
    )
    summary_df = current["summary"].copy()
    status_df = current["status_breakdown"].copy()

    prepared_source = cross_budget_pairing.prepare_cross_budget_source(
        source_transactions_df,
        source_category=source_category_name,
    )
    prepared_target = cross_budget_pairing.prepare_cross_budget_target(
        target_transactions_df,
        target_account=target_account_name,
    )
    full_match = cross_budget_pairing.match_cross_budget_rows(
        source_transactions_df,
        target_transactions_df,
        target_account=target_account_name,
        source_category=source_category_name,
        date_tolerance_days=int(date_tolerance_days),
    )
    month_report = build_cross_budget_month_report(
        source_month_details=source_month_details,
        target_transactions_df=target_transactions_df,
        source_category_name=source_category_name,
        target_account_name=target_account_name,
    ) if source_month_report_df is None else build_cross_budget_month_report_from_source_history(
        source_month_report_df=source_month_report_df,
        target_transactions_df=target_transactions_df,
        target_account_name=target_account_name,
    )
    source_report = _build_source_reconcile_report(
        source_df=prepared_source,
        match_result=full_match,
    )
    target_report = _build_cross_budget_target_report(
        target_df=prepared_target,
        match_result=full_match,
    )

    source_report["date_ts"] = pd.to_datetime(source_report["date"], errors="coerce").dt.normalize()
    target_report["date_ts"] = pd.to_datetime(target_report["date"], errors="coerce").dt.normalize()
    since_ts = _parse_optional_date(since)
    if since_ts is None and not source_report.empty:
        since_ts = source_report["date_ts"].min()

    summary_df.loc[0, "since"] = since_ts.strftime("%Y-%m-%d") if since_ts is not None else ""
    summary_df.loc[0, "anchor_streak"] = int(anchor_streak)

    if month_report.empty:
        summary_df.loc[0, "reason"] = "No month-balance history was available for the Family source category."
        return {
            "ok": False,
            "reason": summary_df.loc[0, "reason"],
            "summary": summary_df,
            "status_breakdown": status_df,
            "month_report": month_report,
            "source_report": source_report.drop(columns=["date_ts"], errors="ignore"),
            "target_report": target_report.drop(columns=["date_ts"], errors="ignore"),
            "matched_pairs": pd.DataFrame(),
            "unmatched_source": pd.DataFrame(),
            "unmatched_target": pd.DataFrame(),
            "ambiguous_matches": pd.DataFrame(),
            "updates": [],
            "update_count": 0,
        }

    month_report = month_report.copy()
    month_report["month_end_ts"] = pd.to_datetime(month_report["month_end"], errors="coerce").dt.normalize()
    candidate_months = month_report.loc[month_report["is_exact_balance_anchor"].astype(bool)].copy()
    if since_ts is not None:
        candidate_months = candidate_months.loc[candidate_months["month_end_ts"] < since_ts].copy()
    candidate_months = candidate_months.sort_values("month_end_ts", ascending=False)

    best_reason = ""
    chosen_month: dict[str, Any] | None = None
    chosen_pre_source = pd.DataFrame()
    chosen_pre_target = pd.DataFrame()
    chosen_anchor_source = pd.DataFrame()
    chosen_active_source = pd.DataFrame()
    chosen_active_target = pd.DataFrame()

    for _, month_row in candidate_months.iterrows():
        month_end_ts = month_row["month_end_ts"]
        post_source = source_report.loc[source_report["date_ts"] > month_end_ts].copy()
        post_target = target_report.loc[target_report["date_ts"] > month_end_ts].copy()

        if since_ts is not None:
            pre_source = post_source.loc[post_source["date_ts"] < since_ts].copy()
            pre_target = post_target.loc[post_target["date_ts"] < since_ts].copy()
            active_source = post_source.loc[post_source["date_ts"] >= since_ts].copy()
            active_target = post_target.loc[post_target["date_ts"] >= since_ts].copy()
            if len(pre_source) < anchor_streak:
                if not best_reason:
                    best_reason = (
                    f"Latest exact-balance month {month_row['month']} does not leave "
                    f"{anchor_streak} settled source rows before {since_ts.strftime('%Y-%m-%d')}."
                    )
                continue
            anchor_source = pre_source.tail(anchor_streak).copy()
        else:
            if len(post_source) < anchor_streak:
                if not best_reason:
                    best_reason = (
                    f"Latest exact-balance month {month_row['month']} does not leave "
                    f"{anchor_streak} source rows after the anchor month."
                    )
                continue
            anchor_source = post_source.head(anchor_streak).copy()
            pre_source = anchor_source.copy()
            pre_target = post_target.loc[
                post_target["target_row_id"].isin(anchor_source["target_row_id"].astype("string").fillna(""))
            ].copy()
            active_source = post_source.iloc[anchor_streak:].copy()
            active_target = post_target.loc[
                ~post_target["target_row_id"].isin(pre_target["target_row_id"].astype("string").fillna(""))
            ].copy()

        if not _rows_all_reconciled_match(pre_source):
            unsettled = pre_source.loc[
                ~(
                    pre_source["match_status"].astype("string").fillna("").eq("matched_existing")
                    & pre_source["ynab_cleared"].astype("string").fillna("").eq("reconciled")
                )
            ].copy()
            if not best_reason:
                best_reason = (
                    f"History after exact-balance month {month_row['month']} is not fully settled before "
                    f"{since_ts.strftime('%Y-%m-%d') if since_ts is not None else 'the active window'}; "
                    f"first source problem: {_first_problem_row(unsettled, date_col='date', description_col='payee_raw')}."
                )
            continue
        if not _target_rows_all_reconciled_match(pre_target):
            unsettled = pre_target.loc[
                ~(
                    pre_target["match_status"].astype("string").fillna("").eq("matched_existing")
                    & pre_target["cleared"].astype("string").fillna("").eq("reconciled")
                )
            ].copy()
            if not best_reason:
                best_reason = (
                    f"Pilates target history after exact-balance month {month_row['month']} is not fully settled before "
                    f"{since_ts.strftime('%Y-%m-%d') if since_ts is not None else 'the active window'}; "
                    f"first target problem: {_first_problem_row(unsettled, date_col='date', description_col='payee_raw')}."
                )
            continue
        if not _rows_all_reconciled_match(anchor_source):
            if not best_reason:
                best_reason = (
                    f"Exact-balance month {month_row['month']} does not provide a clean reconciled anchor streak."
                )
            continue

        chosen_month = month_row.to_dict()
        chosen_pre_source = pre_source
        chosen_pre_target = pre_target
        chosen_anchor_source = anchor_source
        chosen_active_source = active_source
        chosen_active_target = active_target
        break

    active_match = cross_budget_pairing.CrossBudgetMatchResult(
        matched_pairs_df=pd.DataFrame(),
        unmatched_source_df=pd.DataFrame(),
        unmatched_target_df=pd.DataFrame(),
        ambiguous_matches_df=pd.DataFrame(),
    )
    if since_ts is not None:
        active_source_df = source_transactions_df.loc[
            pd.to_datetime(source_transactions_df["date"], errors="coerce") >= since_ts
        ].copy()
        active_target_df = target_transactions_df.loc[
            pd.to_datetime(target_transactions_df["date"], errors="coerce") >= since_ts
        ].copy()
        active_match = cross_budget_pairing.match_cross_budget_rows(
            active_source_df,
            active_target_df,
            target_account=target_account_name,
            source_category=source_category_name,
            date_tolerance_days=int(date_tolerance_days),
        )

    if chosen_month is None:
        summary_df.loc[0, "anchor_month"] = candidate_months.iloc[0]["month"] if not candidate_months.empty else ""
        summary_df.loc[0, "anchor_balance_ils"] = (
            float(candidate_months.iloc[0]["source_category_balance_ils"]) if not candidate_months.empty else 0.0
        )
        summary_df.loc[0, "matched_pairs_count"] = len(active_match.matched_pairs_df)
        summary_df.loc[0, "unmatched_source_count"] = len(active_match.unmatched_source_df)
        summary_df.loc[0, "unmatched_target_count"] = len(active_match.unmatched_target_df)
        summary_df.loc[0, "ambiguous_count"] = len(active_match.ambiguous_matches_df)
        summary_df.loc[0, "updates_planned"] = 0
        summary_df.loc[0, "reason"] = best_reason or "Could not find an exact-balance anchor month."
        return {
            "ok": False,
            "reason": summary_df.loc[0, "reason"],
            "summary": summary_df,
            "status_breakdown": status_df,
            "month_report": month_report.drop(columns=["month_end_ts"], errors="ignore"),
            "source_report": source_report.drop(columns=["date_ts"], errors="ignore"),
            "target_report": target_report.drop(columns=["date_ts"], errors="ignore"),
            "matched_pairs": active_match.matched_pairs_df,
            "unmatched_source": active_match.unmatched_source_df,
            "unmatched_target": active_match.unmatched_target_df,
            "ambiguous_matches": active_match.ambiguous_matches_df,
            "updates": [],
            "update_count": 0,
        }

    active_source_problem = chosen_active_source.loc[
        ~chosen_active_source["match_status"].astype("string").fillna("").eq("matched_existing")
    ].copy()
    active_target_problem = chosen_active_target.loc[
        ~chosen_active_target["match_status"].astype("string").fillna("").eq("matched_existing")
    ].copy()
    ok = True
    if not active_source_problem.empty:
        ok = False
        summary_df.loc[0, "reason"] = (
            "Active Family source window is not fully matched after the anchor; first source problem: "
            f"{_first_problem_row(active_source_problem, date_col='date', description_col='payee_raw')}."
        )
    elif not active_target_problem.empty:
        ok = False
        summary_df.loc[0, "reason"] = (
            "Active Pilates target window has unmatched rows after the anchor; first target problem: "
            f"{_first_problem_row(active_target_problem, date_col='date', description_col='payee_raw')}."
        )
    else:
        summary_df.loc[0, "reason"] = ""

    source_active_net = round(float(chosen_active_source.get("signed_amount_ils", pd.Series(dtype="float64")).sum()), 2)
    target_active_net = round(float(chosen_active_target.get("signed_amount_ils", pd.Series(dtype="float64")).sum()), 2)
    matched_inflow = (
        active_match.matched_pairs_df["ynab_inflow_ils"]
        if "ynab_inflow_ils" in active_match.matched_pairs_df.columns
        else pd.Series(dtype="float64")
    )
    matched_outflow = (
        active_match.matched_pairs_df["ynab_outflow_ils"]
        if "ynab_outflow_ils" in active_match.matched_pairs_df.columns
        else pd.Series(dtype="float64")
    )
    matched_active_net = round(
        float(
            pd.to_numeric(matched_inflow, errors="coerce").fillna(0.0).sum()
            - pd.to_numeric(matched_outflow, errors="coerce").fillna(0.0).sum()
        ),
        2,
    )
    if ok and abs(source_active_net - target_active_net) >= 0.005:
        ok = False
        summary_df.loc[0, "reason"] = (
            "Active source and target nets do not match after the anchor "
            f"({source_active_net:.2f} vs {target_active_net:.2f} ILS)."
        )
    if ok and abs(source_active_net - matched_active_net) >= 0.005:
        ok = False
        summary_df.loc[0, "reason"] = (
            "Matched active target rows do not cover the active Family source net after the anchor "
            f"({source_active_net:.2f} vs {matched_active_net:.2f} ILS)."
        )

    anchor_source_ids = set(chosen_anchor_source["source_row_id"].astype("string").fillna("").tolist())
    anchor_target_ids = set(chosen_anchor_source["target_row_id"].astype("string").fillna("").tolist())
    pre_window_source_ids = set(chosen_pre_source["source_row_id"].astype("string").fillna("").tolist())
    pre_window_target_ids = set(chosen_pre_target["target_row_id"].astype("string").fillna("").tolist())
    active_source_ids = set(chosen_active_source["source_row_id"].astype("string").fillna("").tolist())
    active_target_ids = set(chosen_active_target["target_row_id"].astype("string").fillna("").tolist())
    source_report_out, target_report_out = _mark_actions(
        source_report=source_report,
        target_report=target_report,
        anchor_source_ids=anchor_source_ids,
        anchor_target_ids=anchor_target_ids,
        pre_window_source_ids=pre_window_source_ids,
        pre_window_target_ids=pre_window_target_ids,
        active_source_ids=active_source_ids,
        active_target_ids=active_target_ids,
    )

    updates: list[dict[str, str]] = []
    if ok:
        seen_ids: set[str] = set()
        reconcile_rows = chosen_active_target.loc[
            chosen_active_target["cleared"].astype("string").fillna("") != "reconciled"
        ].copy()
        for _, row in reconcile_rows.iterrows():
            txn_id = _normalize_text(row.get("ynab_id", ""))
            if not txn_id or txn_id in seen_ids:
                continue
            seen_ids.add(txn_id)
            updates.append({"id": txn_id, "cleared": "reconciled"})

    summary_df.loc[0, "anchor_month"] = chosen_month.get("month", "")
    summary_df.loc[0, "anchor_balance_ils"] = float(chosen_month.get("source_category_balance_ils", 0.0) or 0.0)
    summary_df.loc[0, "pre_window_source_count"] = len(chosen_pre_source)
    summary_df.loc[0, "pre_window_target_count"] = len(chosen_pre_target)
    summary_df.loc[0, "active_source_net_ils"] = source_active_net
    summary_df.loc[0, "active_target_net_ils"] = target_active_net
    summary_df.loc[0, "matched_active_net_ils"] = matched_active_net
    summary_df.loc[0, "matched_pairs_count"] = len(active_match.matched_pairs_df)
    summary_df.loc[0, "unmatched_source_count"] = len(active_match.unmatched_source_df)
    summary_df.loc[0, "unmatched_target_count"] = len(active_match.unmatched_target_df)
    summary_df.loc[0, "ambiguous_count"] = len(active_match.ambiguous_matches_df)
    summary_df.loc[0, "updates_planned"] = len(updates)

    return {
        "ok": ok,
        "reason": _normalize_text(summary_df.loc[0, "reason"]),
        "summary": summary_df,
        "status_breakdown": status_df,
        "month_report": month_report.drop(columns=["month_end_ts"], errors="ignore"),
        "source_report": source_report_out.drop(columns=["date_ts"], errors="ignore"),
        "target_report": target_report_out.drop(columns=["date_ts"], errors="ignore"),
        "matched_pairs": active_match.matched_pairs_df,
        "unmatched_source": active_match.unmatched_source_df,
        "unmatched_target": active_match.unmatched_target_df,
        "ambiguous_matches": active_match.ambiguous_matches_df,
        "updates": updates,
        "update_count": len(updates),
    }
