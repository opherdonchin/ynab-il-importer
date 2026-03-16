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
    return Path(f"{stem}_card_sync_report.csv")


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
    print("Executed: yes" if execute else "Executed: no (dry run)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stamp card_txn_id lineage onto existing YNAB card transactions and clear matches."
    )
    parser.add_argument("--account", required=True, help="Target YNAB card account name.")
    parser.add_argument("--source", required=True, help="Card snapshot (.xlsx or normalized .csv).")
    parser.add_argument(
        "--report-out",
        default="",
        help="CSV path for the sync report. Defaults to <source>_card_sync_report.csv.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="PATCH YNAB transactions after writing the dry-run report.",
    )
    args = parser.parse_args()

    source_path = Path(args.source)
    report_path = Path(args.report_out) if args.report_out else _default_report_out(source_path)

    source_df = card_reconciliation.load_card_source(source_path)
    accounts = ynab_api.fetch_accounts()
    transactions = ynab_api.fetch_transactions()

    result = card_reconciliation.plan_card_match_sync(
        account_name=args.account,
        source_df=source_df,
        accounts=accounts,
        transactions=transactions,
    )
    export.write_dataframe(result["report"], report_path)
    _print_summary(result, report_path, execute=args.execute)

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(result["updates"])
        print(f"Patched transactions: {len(response.get('transactions', []) or [])}")


if __name__ == "__main__":
    main()
