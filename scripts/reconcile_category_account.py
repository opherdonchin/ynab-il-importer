import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.context_config as context_config
import ynab_il_importer.export as export
import ynab_il_importer.upload_prep as upload_prep
import ynab_il_importer.ynab_api as ynab_api
import ynab_il_importer.ynab_category_reconciliation as category_reconciliation


CATEGORY_SOURCE_KINDS = {"ynab_category"}


def _print_summary(result: dict[str, object], report_path: Path, execute: bool) -> None:
    report = result["report"]
    print(export.wrote_message(report_path, len(report)))
    print(
        "Source category: "
        f"{result['source_category_name']} ({result['source_category_id']})"
    )
    print(
        f"Target account: {result['target_account_name']} ({result['target_account_id']})"
    )
    print(
        "Balances: "
        f"source {result['source_category_balance_ils']:.2f} | "
        f"target {result['target_account_balance_ils']:.2f} | "
        f"cleared {result['target_account_cleared_balance_ils']:.2f} | "
        f"uncleared {result['target_account_uncleared_balance_ils']:.2f}"
    )
    if result["ok"]:
        print(f"Reviewed rows: {result['reviewed_row_count']}")
        print(f"Resolved live rows: {result['resolved_count']}")
        print(f"Updates planned: {result['update_count']}")
        if not report.empty:
            print(f"Already reconciled rows: {result['already_reconciled_count']}")
            print(f"Skipped rows: {result['skipped_count']}")
        print("Executed: yes" if execute else "Executed: no (dry run)")
        return

    print("Status: blocked")
    if result["reason"]:
        print(f"Reason: {result['reason']}")
    if not report.empty:
        print(f"Reviewed rows: {result['reviewed_row_count']}")
        print(f"Blocked rows: {result['blocked_count']}")
        print(f"Resolvable rows: {result['resolved_count']}")
    print("Executed: no (blocked)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile one YNAB-category source against its target account for one "
            "context/run-tag pair."
        )
    )
    parser.add_argument("context", help="Context name, for example: aikido")
    parser.add_argument("run_tag", help="Run folder name, for example: 2026_04_01")
    parser.add_argument(
        "--source-id",
        default="",
        help="Declared context source id when a context has multiple ynab_category sources.",
    )
    parser.add_argument(
        "--reviewed",
        dest="reviewed_path",
        default="",
        help="Reviewed canonical review-artifact path. Defaults to the paired run directory.",
    )
    parser.add_argument(
        "--report-out",
        default="",
        help="CSV path for the reconciliation report. Defaults to the paired run directory.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="PATCH eligible YNAB transactions to cleared=reconciled after validation passes.",
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
        help="Override target YNAB budget id instead of resolving it from the context env binding.",
    )
    args = parser.parse_args()

    defaults = context_config.load_defaults(args.defaults_path)
    context = context_config.load_context(args.context, contexts_root=args.contexts_root)
    run_paths = context_config.resolve_run_paths(defaults, run_tag=args.run_tag)
    selected_sources = context_config.select_context_sources(
        context,
        source_id=args.source_id or None,
        allowed_kinds=CATEGORY_SOURCE_KINDS,
    )
    if len(selected_sources) != 1:
        raise ValueError(
            f"Context {context.name!r} must resolve to exactly one ynab_category source, "
            f"found {[source.id for source in selected_sources]}."
        )
    source = selected_sources[0]
    report_path = (
        Path(args.report_out)
        if args.report_out
        else run_paths.category_account_reconcile_report_path(
            defaults, context.name, source.id
        )
    )
    reviewed_path = (
        Path(args.reviewed_path)
        if args.reviewed_path
        else run_paths.reviewed_review_path(defaults, context.name)
    )

    target_plan_id = context_config.resolve_context_budget_id(
        context, budget_id=args.budget_id
    )
    source_context = context_config.load_context(
        source.from_context, contexts_root=args.contexts_root
    )
    source_plan_id = context_config.resolve_context_budget_id(source_context)
    run_month = category_reconciliation.run_month_from_tag(args.run_tag)

    reviewed = upload_prep.load_upload_working_frame(reviewed_path)
    target_accounts = ynab_api.fetch_accounts(plan_id=target_plan_id or None)
    target_account = category_reconciliation.resolve_live_account(
        target_accounts,
        account_id=source.target_account_id,
        account_name=source.target_account_name,
    )
    relevant_review = category_reconciliation.select_review_rows_for_source(
        reviewed,
        source=category_reconciliation.CategoryReconcileSource(
            category_id=source.category_id,
            category_name=source.category_name,
            target_account_id=_normalize_text(target_account.get("id", "")),
            target_account_name=_normalize_text(target_account.get("name", "")),
        ),
    )

    target_transactions = ynab_api.fetch_transactions(plan_id=target_plan_id or None)
    target_category_groups = ynab_api.fetch_categories(plan_id=target_plan_id or None)
    target_categories = ynab_api.categories_to_dataframe(target_category_groups)
    if target_categories.is_empty():
        target_categories = ynab_api.categories_from_transactions_to_dataframe(
            target_transactions
        )
    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=target_accounts,
        categories_df=target_categories,
    )
    prepared_units = upload_prep.assemble_upload_transaction_units(prepared)

    source_month = ynab_api.fetch_month_detail(run_month, plan_id=source_plan_id or None)
    source_category = category_reconciliation.resolve_month_category(
        source_month,
        category_id=source.category_id,
        category_name=source.category_name,
    )

    result = category_reconciliation.plan_category_account_reconciliation(
        relevant_review,
        prepared_units,
        target_transactions=target_transactions,
        target_account=target_account,
        source_category=source_category,
    )
    export.write_dataframe(result["report"], report_path)
    _print_summary(result, report_path, execute=args.execute)

    if not result["ok"]:
        raise SystemExit(1)

    if args.execute and result["updates"]:
        response = ynab_api.update_transactions(
            result["updates"], plan_id=target_plan_id or None
        )
        updated = response.get("transactions", []) or []
        if not updated and response.get("transaction"):
            updated = [response["transaction"]]
        print(f"Patched transactions: {len(updated)}")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


if __name__ == "__main__":
    main()
