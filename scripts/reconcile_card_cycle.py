import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.card_reconciliation as card_reconciliation
import ynab_il_importer.export as export
import ynab_il_importer.ynab_api as ynab_api


def _default_report_out(source_path: Path) -> Path:
    suffix = source_path.suffix or ".csv"
    stem = source_path.with_suffix("") if source_path.suffix else source_path
    return Path(f"{stem}_card_reconcile_report.csv")


def _print_summary(result: dict[str, object], report_path: Path, execute: bool) -> None:
    print(export.wrote_message(report_path, len(result["report"])))
    print(f"Account: {result['account_name']} ({result['account_id']})")
    print(f"Mode: {result['mode']}")
    if result.get("previous_total_ils"):
        print(f"Previous total: {result['previous_total_ils']:.2f} ILS")
    print(f"Current total: {result['source_total_ils']:.2f} ILS")
    if result.get("payment_transfer_card_transaction_id"):
        print(
            "Payment transfer: "
            f"card {result['payment_transfer_card_amount_ils']:.2f} ILS on {result['payment_transfer_card_date']} "
            f"<-> bank {result['payment_transfer_bank_amount_ils']:.2f} ILS on {result['payment_transfer_bank_date']} "
            f"in {result['payment_transfer_bank_account_name']}"
        )

    if not result["ok"]:
        print("Status: blocked")
        if result["reason"]:
            print(f"Reason: {result['reason']}")
        print("Executed: no (blocked)")
        return

    if result.get("warning"):
        print(f"Warning: {result['warning']}")
    print(f"Updates planned: {result['update_count']}")
    print("Executed: yes" if execute else "Executed: no (dry run)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile one card account using a current source file and optional previous finished-month file."
    )
    parser.add_argument("--account", required=True, help="Target YNAB card account name.")
    parser.add_argument("--source", required=True, help="New current card snapshot (.xlsx or normalized .csv).")
    parser.add_argument(
        "--previous",
        default="",
        help="Previous finished-month card snapshot (.xlsx or normalized .csv).",
    )
    parser.add_argument(
        "--report-out",
        default="",
        help="CSV path for the reconciliation report. Defaults to <source>_card_reconcile_report.csv.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="PATCH eligible YNAB transactions after validation passes.",
    )
    args = parser.parse_args()

    source_path = Path(args.source)
    report_path = Path(args.report_out) if args.report_out else _default_report_out(source_path)

    source_df = card_reconciliation.load_card_source(source_path)
    previous_df = card_reconciliation.load_card_source(args.previous) if args.previous else None
    accounts = ynab_api.fetch_accounts()
    transactions = ynab_api.fetch_transactions()

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name=args.account,
        source_df=source_df,
        previous_df=previous_df,
        accounts=accounts,
        transactions=transactions,
    )
    export.write_dataframe(result["report"], report_path)
    _print_summary(result, report_path, execute=args.execute)

    if not result["ok"]:
        raise SystemExit(1)

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(result["updates"])
        print(f"Patched transactions: {len(response.get('transactions', []) or [])}")


if __name__ == "__main__":
    main()
