# ruff: noqa: E402

from __future__ import annotations

import argparse
from datetime import date, timedelta
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.artifacts.transaction_io import (
    read_transactions_polars,
    write_transactions_parquet,
)
from ynab_il_importer.artifacts.transaction_projection import project_top_level_transactions
import ynab_il_importer.context_config as context_config
import ynab_il_importer.export as export
import ynab_il_importer.ynab_api as ynab_api


def _list_budgets() -> None:
    budgets = ynab_api.fetch_budgets()
    if not budgets:
        print("No YNAB budgets returned for this token.")
        return
    print("YNAB budgets available to this token:")
    for budget in budgets:
        budget_id = str(budget.get("id", "") or "").strip()
        name = str(budget.get("name", "") or "").strip()
        last_modified = str(budget.get("last_modified_on", "") or "").strip()
        suffix = f" last_modified={last_modified}" if last_modified else ""
        print(f"- {name}\t{budget_id}{suffix}")


def _filter_canonical_by_date(table, since: str | None, until: str | None):
    projected = project_top_level_transactions(table, drop_splits=False)
    filtered = projected
    if since:
        filtered = filtered.filter(pl.col("date") >= since)
    if until:
        filtered = filtered.filter(pl.col("date") <= until)
    return filtered.to_arrow()


def _infer_source_date_window(
    *,
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    run_tag: str,
    padding_days: int,
) -> tuple[str, str] | None:
    if padding_days < 0:
        raise ValueError("--source-window-padding-days cannot be negative.")

    run_paths = context_config.resolve_run_paths(defaults, run_tag=run_tag)
    min_dates: list[date] = []
    max_dates: list[date] = []
    for source in context.config.sources:
        if source.kind == "ynab_category":
            continue
        if not source.normalized_name:
            raise ValueError(
                f"Context source {source.id!r} must declare normalized_name "
                "to infer a YNAB source window."
            )
        source_path = run_paths.derived_dir / source.normalized_name
        if not source_path.exists():
            raise FileNotFoundError(
                "Cannot infer YNAB source window before normalized source "
                f"artifacts exist. Missing: {source_path}. Run "
                f"`pixi run normalize-context -- {context.name} {run_tag}` first."
            )
        frame = read_transactions_polars(source_path)
        if "date" not in frame.columns:
            raise ValueError(f"{source_path} is missing required date column.")
        dates = (
            frame.select(
                pl.col("date")
                .cast(pl.String, strict=False)
                .fill_null("")
                .str.strip_chars()
                .alias("date")
            )
            .filter(pl.col("date") != "")
        )
        if dates.is_empty():
            continue
        stats = dates.select(
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
        ).row(0, named=True)
        min_dates.append(date.fromisoformat(str(stats["min_date"])))
        max_dates.append(date.fromisoformat(str(stats["max_date"])))

    if not min_dates:
        return None

    since = min(min_dates) - timedelta(days=padding_days)
    until = max(max_dates) + timedelta(days=padding_days)
    return since.isoformat(), until.isoformat()


def _download_context_snapshot(
    *,
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    run_tag: str,
    budget_id: str = "",
    since_date: str = "",
    until_date: str = "",
    infer_source_window: bool = False,
    source_window_padding_days: int = 14,
    out_path: Path | None = None,
) -> None:
    run_paths = context_config.resolve_run_paths(defaults, run_tag=run_tag)
    run_paths.derived_dir.mkdir(parents=True, exist_ok=True)

    if infer_source_window:
        inferred = _infer_source_date_window(
            context=context,
            defaults=defaults,
            run_tag=run_tag,
            padding_days=source_window_padding_days,
        )
        if inferred is not None:
            inferred_since, inferred_until = inferred
            since_date = since_date or inferred_since
            until_date = until_date or inferred_until
            print(
                "Using source-derived YNAB window for "
                f"{context.name}: --since {since_date} --until {until_date}"
            )

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
    parser.add_argument("context", nargs="?", help="Context name, for example: family")
    parser.add_argument(
        "run_tag",
        nargs="?",
        help="Run folder name, for example: 2026_04_01",
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
        help="Override YNAB budget id.",
    )
    parser.add_argument(
        "--list-budgets",
        action="store_true",
        help="List budgets available to YNAB_ACCESS_TOKEN and exit.",
    )
    parser.add_argument("--since", dest="since_date", default="", help="YYYY-MM-DD")
    parser.add_argument("--until", dest="until_date", default="", help="YYYY-MM-DD")
    parser.add_argument(
        "--source-window",
        action="store_true",
        help=(
            "Infer missing --since/--until bounds from the context's normalized "
            "source artifacts for this run."
        ),
    )
    parser.add_argument(
        "--source-window-padding-days",
        type=int,
        default=14,
        help="Days to pad around the inferred source min/max dates (default: 14).",
    )
    parser.add_argument(
        "--out",
        dest="out_path",
        type=Path,
        default=None,
        help="Optional explicit parquet output path.",
    )
    args = parser.parse_args()

    if args.list_budgets:
        _list_budgets()
        return
    if not args.context or not args.run_tag:
        parser.error("context and run_tag are required unless --list-budgets is used.")

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
            infer_source_window=args.source_window,
            source_window_padding_days=args.source_window_padding_days,
        )
    _download_context_snapshot(
        context=context,
        defaults=defaults,
        run_tag=args.run_tag,
        budget_id=args.budget_id,
        since_date=args.since_date,
        until_date=args.until_date,
        infer_source_window=args.source_window,
        source_window_padding_days=args.source_window_padding_days,
        out_path=args.out_path,
    )


if __name__ == "__main__":
    main()
