import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.bank_reconciliation as bank_reconciliation
import ynab_il_importer.export as export
import ynab_il_importer.ynab_api as ynab_api


def _default_report_out(bank_path: Path) -> Path:
    suffix = bank_path.suffix or ".csv"
    stem = bank_path.with_suffix("") if bank_path.suffix else bank_path
    return Path(f"{stem}_sync_report{suffix}")


def _print_summary(result: dict[str, object], report_path: Path, execute: bool) -> None:
    report = result["report"]
    print(f"Report: {report_path}")
    print(f"Account: {result['account_name']} ({result['account_id']})")
    print(f"Matched rows: {result['matched_count']}")
    print(f"Updates planned: {result['update_count']}")
    if not report.empty:
        unmatched = int((report["action"] == "unmatched").sum())
        blocked = int((report["action"] == "blocked").sum())
        print(f"Unmatched rows: {unmatched}")
        print(f"Blocked rows: {blocked}")
    if execute:
        print("Executed: yes")
    else:
        print("Executed: no (dry run)")


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
        "--execute",
        action="store_true",
        help="PATCH YNAB transactions after writing the dry-run report.",
    )
    args = parser.parse_args()

    bank_path = Path(args.bank)
    report_path = Path(args.report_out) if args.report_out else _default_report_out(bank_path)

    bank_df = bank_reconciliation.load_bank_csv(bank_path)
    accounts = ynab_api.fetch_accounts()
    transactions = ynab_api.fetch_transactions()

    result = bank_reconciliation.plan_bank_match_sync(bank_df, accounts, transactions)
    export.write_dataframe(result["report"], report_path)
    _print_summary(result, report_path, execute=args.execute)

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(result["updates"])
        print(f"Patched transactions: {len(response.get('transactions', []) or [])}")


if __name__ == "__main__":
    main()
