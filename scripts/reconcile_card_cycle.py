import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.card_reconciliation as card_reconciliation
import ynab_il_importer.context_config as context_config
import ynab_il_importer.export as export
import ynab_il_importer.ynab_api as ynab_api


CARD_SOURCE_KINDS = {"max", "leumi_card_html"}


def _account_key(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
    return text.strip("_") or "account"


def _print_summary(result: dict[str, object], report_path: Path, execute: bool) -> None:
    print(export.wrote_message(report_path, len(result["report"])))
    print(f"Account: {result['account_name']} ({result['account_id']})")
    print(f"Mode: {result['mode']}")
    if int(result.get("source_filtered_out_count", 0) or 0) > 0:
        print(f"Filtered source rows: {result['source_filtered_out_count']}")
    if int(result.get("previous_filtered_out_count", 0) or 0) > 0:
        print(f"Filtered previous rows: {result['previous_filtered_out_count']}")
    if result.get("previous_total_ils"):
        print(f"Previous total: {result['previous_total_ils']:.2f} ILS")
        print(
            "Previous line match: "
            f"{result.get('matched_previous_count', 0)}/{result.get('previous_row_count', 0)} rows, "
            f"matched YNAB total {result.get('matched_previous_total_ils', 0.0):.2f} ILS"
        )
    print(f"Current total: {result['source_total_ils']:.2f} ILS")
    print(
        "Current line match: "
        f"{result.get('matched_source_count', 0)}/{result.get('source_row_count', 0)} rows, "
        f"matched YNAB total {result.get('matched_source_total_ils', 0.0):.2f} ILS"
    )
    if result.get("payment_transfer_card_transaction_id"):
        print(
            "Payment transfer: "
            f"card {result['payment_transfer_card_amount_ils']:.2f} ILS on {result['payment_transfer_card_date']} "
            f"<-> bank {result['payment_transfer_bank_amount_ils']:.2f} ILS on {result['payment_transfer_bank_date']} "
            f"in {result['payment_transfer_bank_account_name']}"
        )
    if result.get("separately_settled_count"):
        dates_str = ", ".join(result.get("separately_settled_dates", []))
        print(
            f"Separately settled: {result['separately_settled_count']} rows "
            f"(billing dates: {dates_str})"
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
        description="Reconcile one card account using canonical current and optional previous normalized parquets."
    )
    parser.add_argument("context", help="Context name, for example: family")
    parser.add_argument("run_tag", help="Run folder name, for example: 2026_04_01")
    parser.add_argument("--account", required=True, help="Target YNAB card account name.")
    parser.add_argument(
        "--source-id",
        default="",
        help="Declared context source id when a context has multiple card sources.",
    )
    parser.add_argument(
        "--previous",
        default="",
        help="Explicit normalized parquet for the previous finished-month card snapshot.",
    )
    parser.add_argument(
        "--report-out",
        default="",
        help="CSV path for the reconciliation report. Defaults to the paired run directory.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="PATCH eligible YNAB transactions after validation passes.",
    )
    parser.add_argument(
        "--allow-reconciled-source",
        action="store_true",
        dest="allow_reconciled_source",
        help="Skip the block when source rows are already reconciled (e.g. a later cycle ran first).",
    )
    parser.add_argument(
        "--source-date-from",
        default="",
        help="Filter source rows by date >= YYYY-MM-DD (inclusive).",
    )
    parser.add_argument(
        "--source-date-to",
        default="",
        help="Filter source rows by date <= YYYY-MM-DD (inclusive).",
    )
    parser.add_argument(
        "--previous-date-from",
        default="",
        help="Filter previous rows by date >= YYYY-MM-DD (inclusive).",
    )
    parser.add_argument(
        "--previous-date-to",
        default="",
        help="Filter previous rows by date <= YYYY-MM-DD (inclusive).",
    )
    parser.add_argument(
        "--defaults",
        dest="defaults_path",
        type=Path,
        default=context_config.DEFAULTS_PATH,
        help="Defaults TOML path.",
    )
    parser.add_argument(
        "--contexts-root",
        dest="contexts_root",
        type=Path,
        default=context_config.CONTEXTS_ROOT,
        help="Contexts root directory.",
    )
    parser.add_argument(
        "--budget-id",
        dest="budget_id",
        default="",
        help="Override YNAB budget id instead of resolving it from the context env binding.",
    )
    args = parser.parse_args()

    defaults = context_config.load_defaults(args.defaults_path)
    context = context_config.load_context(args.context, contexts_root=args.contexts_root)
    run_paths = context_config.resolve_run_paths(defaults, run_tag=args.run_tag)
    selected_source = context_config.select_context_sources(
        context,
        source_id=args.source_id or None,
        allowed_kinds=CARD_SOURCE_KINDS,
    )
    if len(selected_source) != 1:
        raise ValueError(
            f"Context {context.name!r} must resolve to exactly one card source, found {[source.id for source in selected_source]}."
        )
    source = selected_source[0]
    source_path = context_config.resolve_context_normalized_source_path(
        context,
        run_paths,
        source_id=source.id,
    )
    report_path = (
        Path(args.report_out)
        if args.report_out
        else run_paths.card_reconcile_report_path(
            defaults,
            context.name,
            source.id,
            _account_key(args.account),
        )
    )
    plan_id = context_config.resolve_context_budget_id(context, budget_id=args.budget_id)

    source_df = card_reconciliation.load_card_source(source_path)
    previous_df = (
        card_reconciliation.load_card_source(args.previous) if args.previous else None
    )
    accounts = ynab_api.fetch_accounts(plan_id=plan_id or None)
    transactions = ynab_api.fetch_transactions(plan_id=plan_id or None)

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name=args.account,
        source_df=source_df,
        previous_df=previous_df,
        accounts=accounts,
        transactions=transactions,
        allow_reconciled_source=args.allow_reconciled_source or source.allow_reconciled_source,
        source_date_from=args.source_date_from or None,
        source_date_to=args.source_date_to or None,
        previous_date_from=args.previous_date_from or None,
        previous_date_to=args.previous_date_to or None,
    )
    export.write_dataframe(result["report"], report_path)
    _print_summary(result, report_path, execute=args.execute)

    if not result["ok"]:
        raise SystemExit(1)

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(result["updates"], plan_id=plan_id or None)
        print(f"Patched transactions: {len(response.get('transactions', []) or [])}")


if __name__ == "__main__":
    main()
