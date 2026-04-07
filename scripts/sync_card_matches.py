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
    report = result["report"]
    print(export.wrote_message(report_path, len(report)))
    print(f"Account: {result['account_name']} ({result['account_id']})")
    if int(result.get("source_filtered_out_count", 0) or 0) > 0:
        print(f"Filtered source rows: {result['source_filtered_out_count']}")
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
        description="Stamp card lineage onto existing YNAB card transactions for one context/run-tag pair."
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
        "--report-out",
        default="",
        help="CSV path for the sync report. Defaults to the paired run directory.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="PATCH YNAB transactions after writing the dry-run report.",
    )
    parser.add_argument(
        "--date-from",
        default="",
        help="Filter source rows by date >= YYYY-MM-DD (inclusive).",
    )
    parser.add_argument(
        "--date-to",
        default="",
        help="Filter source rows by date <= YYYY-MM-DD (inclusive).",
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
        else run_paths.card_sync_report_path(
            defaults,
            context.name,
            source.id,
            _account_key(args.account),
        )
    )
    plan_id = context_config.resolve_context_budget_id(context, budget_id=args.budget_id)

    source_df = card_reconciliation.load_card_source(source_path)
    accounts = ynab_api.fetch_accounts(plan_id=plan_id or None)
    transactions = ynab_api.fetch_transactions(plan_id=plan_id or None)

    result = card_reconciliation.plan_card_match_sync(
        account_name=args.account,
        source_df=source_df,
        accounts=accounts,
        transactions=transactions,
        source_date_from=args.date_from or None,
        source_date_to=args.date_to or None,
    )
    export.write_dataframe(result["report"], report_path)
    _print_summary(result, report_path, execute=args.execute)

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(result["updates"], plan_id=plan_id or None)
        print(f"Patched transactions: {len(response.get('transactions', []) or [])}")


if __name__ == "__main__":
    main()
