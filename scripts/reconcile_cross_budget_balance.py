import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.cross_budget_pairing as cross_budget_pairing
import ynab_il_importer.cross_budget_reconciliation as cross_budget_reconciliation
import ynab_il_importer.export as export
import ynab_il_importer.workflow_profiles as workflow_profiles
import ynab_il_importer.ynab_api as ynab_api


def _safe_print(text: object = "") -> None:
    message = str(text)
    try:
        print(message)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(message.encode("utf-8", errors="backslashreplace") + b"\n")


def _default_artifact_root(target_profile: str) -> Path:
    profile_name = str(target_profile or "").strip().lower()
    if profile_name:
        return Path("data/paired") / f"{profile_name}_cross_budget_live"
    return Path("data/paired") / "cross_budget_live"


def _default_sibling(summary_out: Path, suffix_name: str) -> Path:
    suffix = summary_out.suffix or ".csv"
    stem = summary_out.with_suffix("") if summary_out.suffix else summary_out
    return Path(f"{stem}_{suffix_name}{suffix}")


def _signed_amount_ils(df: pd.DataFrame) -> pd.Series:
    inflow = pd.to_numeric(df.get("inflow_ils", 0.0), errors="coerce").fillna(0.0)
    outflow = pd.to_numeric(df.get("outflow_ils", 0.0), errors="coerce").fillna(0.0)
    return (inflow - outflow).round(2)


def _filter_since(df: pd.DataFrame, since: str | None) -> pd.DataFrame:
    if df.empty or not since:
        return df.copy()
    out = df.copy()
    dates = pd.to_datetime(out["date"], errors="coerce")
    return out.loc[dates >= pd.to_datetime(since, errors="coerce")].copy()


def _target_window_rows(target_df: pd.DataFrame, target_account: str) -> pd.DataFrame:
    if target_df.empty:
        return target_df.copy()
    account_name = (
        target_df.get("account_name", pd.Series([""] * len(target_df), index=target_df.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    return target_df.loc[account_name == str(target_account).strip()].copy()


def _build_window_target_report(
    target_window: pd.DataFrame,
    matched_target_ids: set[str],
) -> pd.DataFrame:
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
        "window_match_status",
        "action",
    ]
    if target_window.empty:
        return pd.DataFrame(columns=columns)

    out = target_window.copy()
    out["signed_amount_ils"] = _signed_amount_ils(out)
    ids = out.get("ynab_id", pd.Series([""] * len(out), index=out.index)).astype("string").fillna("")
    matched_mask = ids.isin(matched_target_ids)
    cleared = out.get("cleared", pd.Series([""] * len(out), index=out.index)).astype("string").fillna("")
    out["window_match_status"] = "unmatched_target"
    out.loc[matched_mask, "window_match_status"] = "matched_target"
    out["action"] = "leave_unmatched"
    out.loc[matched_mask & cleared.eq("reconciled"), "action"] = "already_reconciled"
    out.loc[matched_mask & ~cleared.eq("reconciled"), "action"] = "reconcile"
    return out[columns].copy()


def _print_summary(summary_row: dict[str, object]) -> None:
    _safe_print("Cross-budget balance reconciliation")
    if str(summary_row.get("anchor_month", "")).strip():
        _safe_print(f"  Anchor month:                 {summary_row['anchor_month']}")
    if "anchor_balance_ils" in summary_row:
        _safe_print(
            f"  Anchor balance:               {float(summary_row.get('anchor_balance_ils', 0.0) or 0.0):10.2f} ILS"
        )
    _safe_print(
        f"  Family category balance:      {float(summary_row['source_category_balance_ils']):10.2f} ILS"
    )
    _safe_print(
        f"  Pilates cleared balance:      {float(summary_row['target_account_cleared_balance_ils']):10.2f} ILS"
    )
    _safe_print(
        f"  Pilates uncleared balance:    {float(summary_row['target_account_uncleared_balance_ils']):10.2f} ILS"
    )
    _safe_print(f"  Full-balance difference:      {float(summary_row['difference_ils']):10.2f} ILS")
    _safe_print(
        "  Target rows:                  "
        f"reconciled={int(summary_row['target_txn_reconciled_count'])} "
        f"cleared={int(summary_row['target_txn_cleared_count'])} "
        f"uncleared={int(summary_row['target_txn_uncleared_count'])}"
    )
    _safe_print(f"  Updates planned:              {int(summary_row['updates_planned'])}")
    if str(summary_row.get("since", "")).strip():
        _safe_print(f"  Window since:                 {summary_row['since']}")
        if "pre_window_source_count" in summary_row:
            _safe_print(
                "  Settled base rows:            "
                f"source={int(summary_row.get('pre_window_source_count', 0) or 0)} "
                f"target={int(summary_row.get('pre_window_target_count', 0) or 0)}"
            )
        if "active_source_net_ils" in summary_row:
            _safe_print(
                f"  Active source net:            {float(summary_row.get('active_source_net_ils', 0.0) or 0.0):10.2f} ILS"
            )
            _safe_print(
                f"  Active target net:            {float(summary_row.get('active_target_net_ils', 0.0) or 0.0):10.2f} ILS"
            )
            _safe_print(
                f"  Active matched net:           {float(summary_row.get('matched_active_net_ils', 0.0) or 0.0):10.2f} ILS"
            )
        _safe_print(
            "  Active rows:                  "
            f"matched={int(summary_row.get('matched_pairs_count', 0) or 0)} "
            f"unmatched_source={int(summary_row.get('unmatched_source_count', 0) or 0)} "
            f"unmatched_target={int(summary_row.get('unmatched_target_count', 0) or 0)} "
            f"ambiguous={int(summary_row.get('ambiguous_count', 0) or 0)}"
        )
    if str(summary_row.get("reason", "")).strip():
        _safe_print(f"  Status:                       blocked")
        _safe_print(f"  Reason:                       {summary_row['reason']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile a source-budget category against a target-budget account. "
            "With --since, validate a date-window using cross-budget matching and "
            "mark matched target rows as reconciled."
        )
    )
    parser.add_argument("--source-profile", required=True, help="Source workflow profile.")
    parser.add_argument("--source-budget-id", default="", help="Override source YNAB budget/plan id.")
    parser.add_argument("--source-category", required=True, help="Source category name.")
    parser.add_argument("--target-profile", required=True, help="Target workflow profile.")
    parser.add_argument("--target-budget-id", default="", help="Override target YNAB budget/plan id.")
    parser.add_argument("--target-account", required=True, help="Target account name.")
    parser.add_argument("--since", default="", help="Optional reconciliation window start date (YYYY-MM-DD).")
    parser.add_argument(
        "--anchor-streak",
        type=int,
        default=7,
        help="Required count of already-reconciled matched rows in the opening base.",
    )
    parser.add_argument(
        "--date-tolerance-days",
        type=int,
        default=0,
        help="Allow date-window matching within this many days after exact matching fails.",
    )
    parser.add_argument("--out", dest="out_path", type=Path, default=None, help="Summary report CSV path.")
    parser.add_argument(
        "--month-report-out",
        dest="month_report_out_path",
        type=Path,
        default=None,
        help="Month-balance anchor report CSV path.",
    )
    parser.add_argument(
        "--source-month-report-in",
        dest="source_month_report_in_path",
        type=Path,
        default=None,
        help=(
            "Reuse a previous month_report CSV as cached Family-side month history. "
            "This avoids refetching every historical month detail when the source budget has not changed."
        ),
    )
    parser.add_argument(
        "--source-report-out",
        dest="source_report_out_path",
        type=Path,
        default=None,
        help="Source-side reconciliation report CSV path.",
    )
    parser.add_argument(
        "--status-out",
        dest="status_out_path",
        type=Path,
        default=None,
        help="Target status-breakdown CSV path.",
    )
    parser.add_argument(
        "--target-report-out",
        dest="target_report_out_path",
        type=Path,
        default=None,
        help="Target per-transaction reconciliation report CSV path.",
    )
    parser.add_argument(
        "--pairs-out",
        dest="pairs_out_path",
        type=Path,
        default=None,
        help="Matched-pairs CSV path for the reconciliation window.",
    )
    parser.add_argument(
        "--unmatched-source-out",
        dest="unmatched_source_out_path",
        type=Path,
        default=None,
        help="Unmatched source CSV path for the reconciliation window.",
    )
    parser.add_argument(
        "--unmatched-target-out",
        dest="unmatched_target_out_path",
        type=Path,
        default=None,
        help="Unmatched target CSV path for the reconciliation window.",
    )
    parser.add_argument(
        "--ambiguous-out",
        dest="ambiguous_out_path",
        type=Path,
        default=None,
        help="Ambiguous-match CSV path for the reconciliation window.",
    )
    parser.add_argument(
        "--ignore-source-id",
        dest="ignore_source_ids",
        action="append",
        default=[],
        help="Source row id to exclude from cross-budget row matching (for explicit bootstrap/base exceptions).",
    )
    parser.add_argument(
        "--ignore-target-id",
        dest="ignore_target_ids",
        action="append",
        default=[],
        help="Target row id to exclude from cross-budget row matching (for explicit bootstrap/base exceptions).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "PATCH all matched, not-yet-reconciled target-account rows in the reconciliation "
            "window to cleared=reconciled after validation passes."
        ),
    )
    args = parser.parse_args()

    source_profile = workflow_profiles.resolve_profile(args.source_profile)
    target_profile = workflow_profiles.resolve_profile(args.target_profile)
    source_plan_id = workflow_profiles.resolve_budget_id(
        profile=source_profile.name,
        budget_id=args.source_budget_id,
    )
    target_plan_id = workflow_profiles.resolve_budget_id(
        profile=target_profile.name,
        budget_id=args.target_budget_id,
    )

    artifact_root = _default_artifact_root(target_profile.name)
    summary_out = args.out_path or artifact_root / "cross_budget_balance_reconcile_report.csv"
    month_report_out = args.month_report_out_path or _default_sibling(summary_out, "month_report")
    source_report_out = args.source_report_out_path or _default_sibling(summary_out, "source_report")
    status_out = args.status_out_path or _default_sibling(summary_out, "status_breakdown")
    target_report_out = args.target_report_out_path or _default_sibling(summary_out, "target_report")
    pairs_out = args.pairs_out_path or _default_sibling(summary_out, "matched_pairs")
    unmatched_source_out = args.unmatched_source_out_path or _default_sibling(
        summary_out, "unmatched_source"
    )
    unmatched_target_out = args.unmatched_target_out_path or _default_sibling(
        summary_out, "unmatched_target"
    )
    ambiguous_out = args.ambiguous_out_path or _default_sibling(summary_out, "ambiguous_matches")

    source_categories = ynab_api.fetch_categories(plan_id=source_plan_id or None)
    source_accounts = ynab_api.fetch_accounts(plan_id=source_plan_id or None)
    source_transactions = ynab_api.fetch_transactions(plan_id=source_plan_id or None)
    cached_source_month_report = None
    source_months: list[dict[str, object]] = []
    if args.source_month_report_in_path is not None:
        cached_source_month_report = pd.read_csv(args.source_month_report_in_path).fillna("")
    else:
        source_months = [
            ynab_api.fetch_month_detail(month.get("month", ""), plan_id=source_plan_id or None)
            for month in ynab_api.fetch_months(plan_id=source_plan_id or None)
            if str(month.get("month", "")).strip()
        ]
    target_accounts = ynab_api.fetch_accounts(plan_id=target_plan_id or None)
    target_transactions = ynab_api.fetch_transactions(plan_id=target_plan_id or None)

    source_df = ynab_api.category_transactions_to_dataframe(source_transactions, source_accounts)
    target_df = ynab_api.transactions_to_dataframe(target_transactions, target_accounts)
    working_source_df = source_df.copy()
    working_target_df = target_df.copy()
    ignore_source_ids = {
        str(value or "").strip() for value in (args.ignore_source_ids or []) if str(value or "").strip()
    }
    ignore_target_ids = {
        str(value or "").strip() for value in (args.ignore_target_ids or []) if str(value or "").strip()
    }
    if ignore_source_ids and not working_source_df.empty and "ynab_id" in working_source_df.columns:
        source_ids = working_source_df["ynab_id"].astype("string").fillna("").str.strip()
        working_source_df = working_source_df.loc[~source_ids.isin(ignore_source_ids)].copy()
    if ignore_target_ids and not working_target_df.empty and "ynab_id" in working_target_df.columns:
        target_ids = working_target_df["ynab_id"].astype("string").fillna("").str.strip()
        working_target_df = working_target_df.loc[~target_ids.isin(ignore_target_ids)].copy()
    result = cross_budget_reconciliation.plan_cross_budget_reconciliation(
        source_category_groups=source_categories,
        source_month_details=source_months,
        source_month_report_df=cached_source_month_report,
        target_accounts=target_accounts,
        source_transactions_df=working_source_df,
        target_transactions_df=working_target_df,
        source_category_name=args.source_category,
        target_account_name=args.target_account,
        since=args.since or None,
        anchor_streak=int(args.anchor_streak),
        date_tolerance_days=int(args.date_tolerance_days),
        source_profile=source_profile.name,
        target_profile=target_profile.name,
    )
    summary_df = result["summary"].copy()
    summary_df.loc[0, "ignored_source_count"] = len(ignore_source_ids)
    summary_df.loc[0, "ignored_target_count"] = len(ignore_target_ids)
    export.write_dataframe(summary_df, summary_out)
    export.write_dataframe(result["month_report"], month_report_out)
    export.write_dataframe(result["source_report"], source_report_out)
    export.write_dataframe(result["status_breakdown"], status_out)
    export.write_dataframe(result["target_report"], target_report_out)
    export.write_dataframe(result["matched_pairs"], pairs_out)
    export.write_dataframe(result["unmatched_source"], unmatched_source_out)
    export.write_dataframe(result["unmatched_target"], unmatched_target_out)
    export.write_dataframe(result["ambiguous_matches"], ambiguous_out)

    summary_row = summary_df.iloc[0].to_dict()
    _print_summary(summary_row)
    if ignore_source_ids or ignore_target_ids:
        _safe_print(
            "  Ignored bootstrap ids:         "
            f"source={len(ignore_source_ids)} target={len(ignore_target_ids)}"
        )
    print(export.report_message(summary_out))
    print(export.report_message(month_report_out))
    print(export.report_message(source_report_out))
    print(export.report_message(status_out))
    print(export.report_message(target_report_out))
    print(export.report_message(pairs_out))
    print(export.report_message(unmatched_source_out))
    print(export.report_message(unmatched_target_out))
    print(export.report_message(ambiguous_out))

    if not result["ok"]:
        raise ValueError(str(result["reason"]))

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(result["updates"], plan_id=target_plan_id or None)
        updated_rows = response.get("transactions", []) or []
        print(f"Applied reconcile updates: {len(updated_rows) or len(result['updates'])}")
    elif args.execute:
        print("No reconcile updates were needed.")


if __name__ == "__main__":
    main()
