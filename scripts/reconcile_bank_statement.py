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
    return Path(f"{stem}_reconcile_report{suffix}")


def _print_summary(result: dict[str, object], report_path: Path, execute: bool) -> None:
    print(f"Report: {report_path}")
    print(f"Account: {result['account_name']} ({result['account_id']})")
    print(f"Anchor: {result['anchor_type']} at {result['anchor_balance_ils']:.2f} ILS")
    print(f"Updates planned: {result['update_count']}")
    print(f"Final balance: {result['final_balance_ils']:.2f} ILS")
    print("Executed: yes" if execute else "Executed: no (dry run)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile a normalized bank statement against YNAB using bank_txn_id lineage."
    )
    parser.add_argument("--bank", required=True, help="Normalized bank CSV with balance_ils.")
    parser.add_argument(
        "--report-out",
        default="",
        help="CSV path for the reconciliation report. Defaults to <bank>_reconcile_report.csv.",
    )
    parser.add_argument(
        "--anchor-streak",
        type=int,
        default=7,
        help="Required opening streak of exact reconciled matches when last_reconciled_at exists.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="PATCH eligible YNAB transactions to cleared=reconciled after validation passes.",
    )
    args = parser.parse_args()

    bank_path = Path(args.bank)
    report_path = Path(args.report_out) if args.report_out else _default_report_out(bank_path)

    bank_df = bank_reconciliation.load_bank_csv(bank_path)
    accounts = ynab_api.fetch_accounts()
    transactions = ynab_api.fetch_transactions()

    result = bank_reconciliation.plan_bank_statement_reconciliation(
        bank_df,
        accounts,
        transactions,
        anchor_streak=args.anchor_streak,
    )
    export.write_dataframe(result["report"], report_path)
    _print_summary(result, report_path, execute=args.execute)

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(result["updates"])
        print(f"Patched transactions: {len(response.get('transactions', []) or [])}")


if __name__ == "__main__":
    main()
