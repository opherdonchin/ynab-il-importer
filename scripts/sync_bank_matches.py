import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.bank_reconciliation as bank_reconciliation
import ynab_il_importer.context_config as context_config
import ynab_il_importer.export as export
import ynab_il_importer.ynab_api as ynab_api


BANK_SOURCE_KINDS = {"leumi", "leumi_xls"}


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
        description="Stamp bank lineage onto existing YNAB bank transactions for one context/run-tag pair."
    )
    parser.add_argument("context", help="Context name, for example: family")
    parser.add_argument("run_tag", help="Run folder name, for example: 2026_04_01")
    parser.add_argument(
        "--source-id",
        default="",
        help="Declared context source id when a context has multiple bank sources.",
    )
    parser.add_argument(
        "--report-out",
        default="",
        help="CSV path for the sync report. Defaults to the paired run directory.",
    )
    parser.add_argument(
        "--uncleared-report-out",
        default="",
        help="CSV path for the uncleared-YNAB triage report. Defaults to the paired run directory.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="PATCH YNAB transactions after writing the dry-run report.",
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
        allowed_kinds=BANK_SOURCE_KINDS,
    )
    if len(selected_source) != 1:
        raise ValueError(
            f"Context {context.name!r} must resolve to exactly one bank source, found {[source.id for source in selected_source]}."
        )
    source = selected_source[0]
    bank_path = context_config.resolve_context_normalized_source_path(
        context,
        run_paths,
        source_id=source.id,
    )
    report_path = (
        Path(args.report_out)
        if args.report_out
        else run_paths.bank_sync_report_path(defaults, context.name, source.id)
    )
    uncleared_report_path = (
        Path(args.uncleared_report_out)
        if args.uncleared_report_out
        else run_paths.bank_uncleared_report_path(defaults, context.name, source.id)
    )
    plan_id = context_config.resolve_context_budget_id(context, budget_id=args.budget_id)

    bank_df = bank_reconciliation.load_bank_transactions(bank_path)
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
