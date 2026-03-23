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
    print("Cross-budget balance reconciliation")
    print(
        f"  Family category balance:      {float(summary_row['source_category_balance_ils']):10.2f} ILS"
    )
    print(
        f"  Pilates cleared balance:      {float(summary_row['target_account_cleared_balance_ils']):10.2f} ILS"
    )
    print(
        f"  Pilates uncleared balance:    {float(summary_row['target_account_uncleared_balance_ils']):10.2f} ILS"
    )
    print(f"  Full-balance difference:      {float(summary_row['difference_ils']):10.2f} ILS")
    print(
        "  Target rows:                  "
        f"reconciled={int(summary_row['target_txn_reconciled_count'])} "
        f"cleared={int(summary_row['target_txn_cleared_count'])} "
        f"uncleared={int(summary_row['target_txn_uncleared_count'])}"
    )
    print(f"  Updates planned:              {int(summary_row['updates_planned'])}")
    if str(summary_row.get("since", "")).strip():
        print(f"  Window since:                 {summary_row['since']}")
        print(
            f"  Window source net:            {float(summary_row['source_window_net_ils']):10.2f} ILS"
        )
        print(
            f"  Window target net:            {float(summary_row['target_window_net_ils']):10.2f} ILS"
        )
        print(
            f"  Window matched net:           {float(summary_row['matched_target_window_net_ils']):10.2f} ILS"
        )
        print(
            "  Window rows:                  "
            f"matched={int(summary_row['matched_pairs_count'])} "
            f"unmatched_source={int(summary_row['unmatched_source_count'])} "
            f"unmatched_target={int(summary_row['unmatched_target_count'])} "
            f"ambiguous={int(summary_row['ambiguous_count'])}"
        )
        print(
            f"  Opening-gap after execute:    {float(summary_row['opening_gap_ils']):10.2f} ILS"
        )


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
    parser.add_argument("--out", dest="out_path", type=Path, default=None, help="Summary report CSV path.")
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
    target_accounts = ynab_api.fetch_accounts(plan_id=target_plan_id or None)
    target_transactions = ynab_api.fetch_transactions(plan_id=target_plan_id or None)

    source_df = ynab_api.transactions_to_dataframe(source_transactions, source_accounts)
    target_df = ynab_api.transactions_to_dataframe(target_transactions, target_accounts)
    full_result = cross_budget_reconciliation.build_cross_budget_balance_report(
        source_category_groups=source_categories,
        target_accounts=target_accounts,
        target_transactions_df=target_df,
        source_category_name=args.source_category,
        target_account_name=args.target_account,
        source_profile=source_profile.name,
        target_profile=target_profile.name,
    )
    summary_df = full_result["summary"].copy()
    status_df = full_result["status_breakdown"].copy()
    target_report_df = full_result["target_report"].copy()
    updates = cross_budget_reconciliation.planned_reconciliation_actions(full_result["target_transactions"])
    blocking_issues: list[str] = []

    if args.since:
        source_window = _filter_since(source_df, args.since)
        target_window = _filter_since(target_df, args.since)
        window_match = cross_budget_pairing.match_cross_budget_rows(
            source_window,
            target_window,
            target_account=args.target_account,
            source_category=args.source_category,
            date_tolerance_days=0,
        )

        export.write_dataframe(window_match.matched_pairs_df, pairs_out)
        export.write_dataframe(window_match.unmatched_source_df, unmatched_source_out)
        export.write_dataframe(window_match.unmatched_target_df, unmatched_target_out)
        export.write_dataframe(window_match.ambiguous_matches_df, ambiguous_out)

        source_window_mask = (
            source_window.get(
                "category_raw",
                pd.Series([""] * len(source_window), index=source_window.index),
            )
            .astype("string")
            .fillna("")
            .str.strip()
            .eq(str(args.source_category).strip())
        )
        source_window_net = round(float(_signed_amount_ils(source_window.loc[source_window_mask]).sum()), 2)
        target_window_only = _target_window_rows(target_window, args.target_account)
        target_window_net = round(float(_signed_amount_ils(target_window_only).sum()), 2)
        matched_target_net = round(
            float(
                pd.to_numeric(window_match.matched_pairs_df.get("ynab_inflow_ils", 0.0), errors="coerce")
                .fillna(0.0)
                .sum()
                - pd.to_numeric(window_match.matched_pairs_df.get("ynab_outflow_ils", 0.0), errors="coerce")
                .fillna(0.0)
                .sum()
            ),
            2,
        )
        matched_target_ids = set(
            window_match.matched_pairs_df.get("ynab_id", pd.Series(dtype="string"))
            .astype("string")
            .fillna("")
            .tolist()
        )
        target_report_df = _build_window_target_report(target_window_only, matched_target_ids)
        updates = [
            {"id": str(row["ynab_id"]), "cleared": "reconciled"}
            for _, row in target_report_df.iterrows()
            if str(row.get("action", "")) == "reconcile" and str(row.get("ynab_id", "")).strip()
        ]
        newly_cleared_net = round(
            float(
                target_report_df.loc[
                    (target_report_df["action"] == "reconcile")
                    & target_report_df["cleared"].astype("string").fillna("").eq("uncleared"),
                    "signed_amount_ils",
                ].sum()
            ),
            2,
        )
        current_cleared_balance = float(summary_df.loc[0, "target_account_cleared_balance_ils"])
        projected_cleared_after_execute = round(current_cleared_balance + newly_cleared_net, 2)
        opening_gap = round(
            projected_cleared_after_execute - float(summary_df.loc[0, "source_category_balance_ils"]),
            2,
        )

        summary_df.loc[0, "since"] = str(args.since).strip()
        summary_df.loc[0, "source_window_net_ils"] = source_window_net
        summary_df.loc[0, "target_window_net_ils"] = target_window_net
        summary_df.loc[0, "matched_target_window_net_ils"] = matched_target_net
        summary_df.loc[0, "window_difference_ils"] = round(source_window_net - target_window_net, 2)
        summary_df.loc[0, "matched_window_difference_ils"] = round(
            source_window_net - matched_target_net, 2
        )
        summary_df.loc[0, "matched_pairs_count"] = len(window_match.matched_pairs_df)
        summary_df.loc[0, "unmatched_source_count"] = len(window_match.unmatched_source_df)
        summary_df.loc[0, "unmatched_target_count"] = len(window_match.unmatched_target_df)
        summary_df.loc[0, "ambiguous_count"] = len(window_match.ambiguous_matches_df)
        summary_df.loc[0, "updates_planned"] = len(updates)
        summary_df.loc[0, "projected_cleared_after_execute_ils"] = projected_cleared_after_execute
        summary_df.loc[0, "opening_gap_ils"] = opening_gap

        if abs(float(summary_df.loc[0, "window_difference_ils"])) >= 0.005:
            blocking_issues.append(
                "Family source net and Pilates target net do not match within the reconciliation window."
            )
        if abs(float(summary_df.loc[0, "matched_window_difference_ils"])) >= 0.005:
            blocking_issues.append(
                "Matched target rows do not cover the full Family source net within the reconciliation window."
            )
        if len(window_match.unmatched_source_df):
            blocking_issues.append(
                f"{len(window_match.unmatched_source_df)} Family source rows remain unmatched in the reconciliation window."
            )
        if len(window_match.unmatched_target_df):
            blocking_issues.append(
                f"{len(window_match.unmatched_target_df)} Pilates target rows remain unmatched in the reconciliation window."
            )
        if len(window_match.ambiguous_matches_df):
            blocking_issues.append(
                f"{len(window_match.ambiguous_matches_df)} ambiguous match buckets remain in the reconciliation window."
            )

    export.write_dataframe(summary_df, summary_out)
    export.write_dataframe(status_df, status_out)
    export.write_dataframe(target_report_df, target_report_out)

    summary_row = summary_df.iloc[0].to_dict()
    _print_summary(summary_row)
    print(export.report_message(summary_out))
    print(export.report_message(status_out))
    print(export.report_message(target_report_out))
    if args.since:
        print(export.report_message(pairs_out))
        print(export.report_message(unmatched_source_out))
        print(export.report_message(unmatched_target_out))
        print(export.report_message(ambiguous_out))
        if float(summary_df.loc[0, "opening_gap_ils"]) != 0.0:
            print(
                "Warning: a legacy opening-gap remains before the reconciliation window "
                f"({float(summary_df.loc[0, 'opening_gap_ils']):.2f} ILS)."
            )
    else:
        issues = cross_budget_reconciliation.reconciliation_issues(summary_df)
        if issues:
            raise ValueError(" ".join(issues))

    if blocking_issues:
        raise ValueError(" ".join(blocking_issues))

    if args.execute and updates:
        response = ynab_api.update_transactions(updates, plan_id=target_plan_id or None)
        updated_rows = response.get("transactions", []) or []
        print(f"Applied reconcile updates: {len(updated_rows) or len(updates)}")
    elif args.execute:
        print("No reconcile updates were needed.")


if __name__ == "__main__":
    main()
