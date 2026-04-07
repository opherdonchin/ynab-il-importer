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
    print(export.wrote_message(report_path, len(result["report"])))
    print(f"Account: {result['account_name']} ({result['account_id']})")
    print(f"Anchor mode: {result['anchor_type'] or 'unknown'}")
    if result["ok"]:
        print(f"Anchor: {result['anchor_balance_ils']:.2f} ILS")
        print(f"Updates planned: {result['update_count']}")
        print(f"Final balance: {result['final_balance_ils']:.2f} ILS")
        print("Executed: yes" if execute else "Executed: no (dry run)")
        return

    print("Status: blocked")
    if result["reason"]:
        print(f"Reason: {result['reason']}")
    if result.get("last_reconciled_at"):
        print(
            f"YNAB account last_reconciled_at: {result['last_reconciled_at']} "
            "(updated only by YNAB's native reconciliation wizard, not by API patching)"
        )
    print(f"Exact lineage matches in file: {result['matched_count']}")
    print(f"Already reconciled matches in file: {result['reconciled_match_count']}")
    if int(result["probable_legacy_match_count"] or 0) > 0:
        print(
            "Probable legacy reconciled matches in file: "
            f"{result['probable_legacy_match_count']}"
        )
    if int(result["anchor_expected_count"] or 0) > 0:
        window_label = (
            f"rows {result['anchor_window_row_start']}..{result['anchor_window_row_end']}"
            if int(result["anchor_window_row_start"]) >= 0
            else "current window"
        )
        print(
            f"Anchor window {window_label}: "
            f"eligible {result['anchor_eligible_count']} / {result['anchor_expected_count']}, "
            f"exact lineage {result['anchor_matched_count']} / {result['anchor_expected_count']}, "
            f"reconciled exact {result['anchor_reconciled_count']} / {result['anchor_expected_count']}"
        )
        if int(result["anchor_probable_legacy_count"] or 0) > 0:
            print(
                "Probable legacy anchors in window: "
                f"{result['anchor_probable_legacy_count']} / {result['anchor_expected_count']}"
            )
    if int(result["post_anchor_unresolved_count"] or 0) > 0:
        print(
            "Post-anchor unresolved rows: "
            f"{result['post_anchor_unresolved_count']} "
            f"(first at row {result['first_post_anchor_unresolved_row']})"
        )
    print("Executed: no (blocked)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile one canonical bank statement against YNAB for one context/run-tag pair."
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
        help="CSV path for the reconciliation report. Defaults to the paired run directory.",
    )
    parser.add_argument(
        "--anchor-streak",
        type=int,
        default=7,
        help="Required opening streak of exact reconciled matches when last_reconciled_at exists.",
    )
    parser.add_argument(
        "--use-ynab-reconciled-date",
        action="store_true",
        default=False,
        help=(
            "Enforce the last_reconciled_at date guard from the YNAB account. "
            "By default this is ignored because YNAB only updates that field via "
            "its native reconciliation wizard, not through API patching."
        ),
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
        else run_paths.bank_reconcile_report_path(defaults, context.name, source.id)
    )
    plan_id = context_config.resolve_context_budget_id(context, budget_id=args.budget_id)

    bank_df = bank_reconciliation.load_bank_transactions(bank_path)
    accounts = ynab_api.fetch_accounts(plan_id=plan_id or None)
    transactions = ynab_api.fetch_transactions(plan_id=plan_id or None)

    result = bank_reconciliation.plan_bank_statement_reconciliation(
        bank_df,
        accounts,
        transactions,
        anchor_streak=args.anchor_streak,
        use_ynab_reconciled_date=args.use_ynab_reconciled_date,
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
