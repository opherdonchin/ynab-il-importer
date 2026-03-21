import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.bank_reconciliation as bank_reconciliation
import ynab_il_importer.export as export
import ynab_il_importer.workflow_profiles as workflow_profiles
import ynab_il_importer.ynab_api as ynab_api


def _default_report_out(bank_path: Path) -> Path:
    suffix = bank_path.suffix or ".csv"
    stem = bank_path.with_suffix("") if bank_path.suffix else bank_path
    return Path(f"{stem}_sync_report{suffix}")


def _default_uncleared_report_out(bank_path: Path) -> Path:
    suffix = bank_path.suffix or ".csv"
    stem = bank_path.with_suffix("") if bank_path.suffix else bank_path
    return Path(f"{stem}_uncleared_ynab_report{suffix}")


def _print_summary(result: dict[str, object], report_path: Path, execute: bool) -> None:
    report = result["report"]
    print(export.wrote_message(report_path, len(report)))
    print(f"Account: {result['account_name']} ({result['account_id']})")
    print(f"Matched rows: {result['matched_count']}")
    print(f"Updates planned: {result['update_count']}")
    if not report.empty:
        unmatched = int((report["action"] == "unmatched").sum())
        blocked = int((report["action"] == "blocked").sum())
        print(f"Unmatched rows: {unmatched}")
        print(f"Blocked rows: {blocked}")
        if unmatched:
            top_unmatched = (
                report.loc[report["action"] == "unmatched", "candidate_status"]
                .astype("string")
                .fillna("")
                .str.strip()
                .replace("", "<unspecified>")
                .value_counts()
                .head(5)
            )
            if not top_unmatched.empty:
                print("Top unmatched reasons:")
                for status, count in top_unmatched.items():
                    print(f"  {status}: {count}")
    if execute:
        print("Executed: yes")
    else:
        print("Executed: no (dry run)")


def _print_uncleared_summary(result: dict[str, object], report_path: Path) -> None:
    report = result["report"]
    print(export.wrote_message(report_path, len(report)))
    print("Outstanding uncleared YNAB rows:")
    print(f"  recent_pending: {result['recent_pending_count']}")
    print(f"  candidate_source_match: {result['candidate_source_match_count']}")
    print(f"  stale_orphan: {result['stale_orphan_count']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stamp bank_txn_id lineage onto existing YNAB bank transactions and clear matches."
    )
    parser.add_argument("--bank", required=True, help="Normalized bank CSV with bank_txn_id.")
    parser.add_argument(
        "--report-out",
        default="",
        help="CSV path for the sync report. Defaults to <bank>_sync_report.csv.",
    )
    parser.add_argument(
        "--uncleared-report-out",
        default="",
        help=(
            "CSV path for the uncleared-YNAB triage report. "
            "Defaults to <bank>_uncleared_ynab_report.csv."
        ),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="PATCH YNAB transactions after writing the dry-run report.",
    )
    parser.add_argument("--profile", default="", help="Workflow profile (for budget defaults).")
    parser.add_argument("--budget-id", dest="budget_id", default="", help="Override YNAB budget/plan id.")
    args = parser.parse_args()

    bank_path = Path(args.bank)
    report_path = Path(args.report_out) if args.report_out else _default_report_out(bank_path)
    uncleared_report_path = (
        Path(args.uncleared_report_out)
        if args.uncleared_report_out
        else _default_uncleared_report_out(bank_path)
    )
    profile = workflow_profiles.resolve_profile(args.profile or None)
    plan_id = workflow_profiles.resolve_budget_id(
        profile=profile.name,
        budget_id=args.budget_id,
    )

    bank_df = bank_reconciliation.load_bank_csv(bank_path)
    accounts = ynab_api.fetch_accounts(plan_id=plan_id or None)
    transactions = ynab_api.fetch_transactions(plan_id=plan_id or None)

    result = bank_reconciliation.plan_bank_match_sync(bank_df, accounts, transactions)
    uncleared_result = bank_reconciliation.plan_uncleared_ynab_triage(
        bank_df, accounts, transactions
    )
    export.write_dataframe(result["report"], report_path)
    export.write_dataframe(uncleared_result["report"], uncleared_report_path)
    _print_summary(result, report_path, execute=args.execute)
    _print_uncleared_summary(uncleared_result, uncleared_report_path)

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(result["updates"], plan_id=plan_id or None)
        print(f"Patched transactions: {len(response.get('transactions', []) or [])}")


if __name__ == "__main__":
    main()
