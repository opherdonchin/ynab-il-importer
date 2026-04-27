# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.artifacts.transaction_io import write_transactions_parquet
from ynab_il_importer.artifacts.transaction_projection import project_top_level_transactions
import ynab_il_importer.context_config as context_config
import ynab_il_importer.export as export
import ynab_il_importer.ynab_api as ynab_api


def _filter_canonical_by_date(table, since: str | None, until: str | None):
    projected = project_top_level_transactions(table, drop_splits=False)
    filtered = projected
    if since:
        filtered = filtered.filter(pl.col("date") >= since)
    if until:
        filtered = filtered.filter(pl.col("date") <= until)
    return filtered.to_arrow()


def _download_context_snapshot(
    *,
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    run_tag: str,
    budget_id: str = "",
    since_date: str = "",
    until_date: str = "",
    out_path: Path | None = None,
) -> None:
    run_paths = context_config.resolve_run_paths(defaults, run_tag=run_tag)
    run_paths.derived_dir.mkdir(parents=True, exist_ok=True)

    plan_id = context_config.resolve_context_budget_id(context, budget_id=budget_id)
    resolved_out_path = out_path or (run_paths.derived_dir / context.ynab_normalized_name)
    if resolved_out_path.suffix.lower() != ".parquet":
        raise ValueError(f"YNAB snapshot output must be parquet: {resolved_out_path}")

    accounts = ynab_api.fetch_accounts(plan_id=plan_id or None)
    txns = ynab_api.fetch_transactions(
        plan_id=plan_id or None,
        since_date=since_date or None,
    )
    canonical = ynab_api.transactions_to_canonical_table(txns, accounts)
    canonical = _filter_canonical_by_date(
        canonical,
        since_date or None,
        until_date or None,
    )

    write_transactions_parquet(canonical, resolved_out_path)
    print(export.wrote_message(resolved_out_path, canonical.num_rows))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download the declared YNAB snapshot for one context/run-tag pair."
    )
    parser.add_argument("context", help="Context name, for example: family")
    parser.add_argument("run_tag", help="Run folder name, for example: 2026_04_01")
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
        help="Override YNAB budget id.",
    )
    parser.add_argument("--since", dest="since_date", default="", help="YYYY-MM-DD")
    parser.add_argument("--until", dest="until_date", default="", help="YYYY-MM-DD")
    parser.add_argument(
        "--out",
        dest="out_path",
        type=Path,
        default=None,
        help="Optional explicit parquet output path.",
    )
    args = parser.parse_args()

    defaults = context_config.load_defaults(args.defaults_path)
    context = context_config.load_context(args.context, contexts_root=args.contexts_root)
    dependency_order = context_config.resolve_context_ynab_dependencies(
        context,
        contexts_root=args.contexts_root,
    )
    for dependency in dependency_order[:-1]:
        _download_context_snapshot(
            context=dependency,
            defaults=defaults,
            run_tag=args.run_tag,
            since_date=args.since_date,
            until_date=args.until_date,
        )
    _download_context_snapshot(
        context=context,
        defaults=defaults,
        run_tag=args.run_tag,
        budget_id=args.budget_id,
        since_date=args.since_date,
        until_date=args.until_date,
        out_path=args.out_path,
    )


if __name__ == "__main__":
    main()
