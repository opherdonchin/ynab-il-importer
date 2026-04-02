# ruff: noqa: E402

import argparse
import sys
from pathlib import Path

import pandas as pd
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.artifacts.transaction_io import write_transactions_parquet
from ynab_il_importer.artifacts.transaction_projection import project_top_level_transactions
import ynab_il_importer.export as export
import ynab_il_importer.workflow_profiles as workflow_profiles
import ynab_il_importer.ynab_api as ynab_api


def _filter_by_date(df: pd.DataFrame, since: str | None, until: str | None) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if since:
        since_date = pd.to_datetime(since, errors="coerce").date()
        out = out[out["date"] >= since_date]
    if until:
        until_date = pd.to_datetime(until, errors="coerce").date()
        out = out[out["date"] <= until_date]
    return out


def _filter_canonical_by_date(table, since: str | None, until: str | None):
    projected = project_top_level_transactions(table, drop_splits=False)
    filtered = projected
    if since:
        filtered = filtered.filter(pl.col("date") >= since)
    if until:
        filtered = filtered.filter(pl.col("date") <= until)
    return filtered.to_arrow()


def _parquet_out_path(csv_path: Path) -> Path:
    return csv_path.with_suffix(".parquet")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download YNAB transactions via API")
    parser.add_argument(
        "--profile", default="", help="Workflow profile (for budget/category defaults)."
    )
    parser.add_argument(
        "--budget-id", dest="budget_id", default="", help="Override YNAB budget/plan id."
    )
    parser.add_argument("--since", dest="since_date", default="", help="YYYY-MM-DD")
    parser.add_argument("--until", dest="until_date", default="", help="YYYY-MM-DD")
    parser.add_argument(
        "--out",
        dest="out_path",
        type=Path,
        default=None,
    )
    args = parser.parse_args()

    profile = workflow_profiles.resolve_profile(args.profile or None)
    plan_id = workflow_profiles.resolve_budget_id(
        profile=profile.name,
        budget_id=args.budget_id,
    )
    out_path = args.out_path or Path("data/derived") / profile.name / "ynab_api_norm.csv"

    accounts = ynab_api.fetch_accounts(plan_id=plan_id or None)
    txns = ynab_api.fetch_transactions(plan_id=plan_id or None, since_date=args.since_date or None)
    canonical = ynab_api.transactions_to_canonical_table(txns, accounts)
    canonical = _filter_canonical_by_date(
        canonical,
        args.since_date or None,
        args.until_date or None,
    )
    df = ynab_api.transactions_to_dataframe(txns, accounts)
    df = _filter_by_date(df, args.since_date or None, args.until_date or None)

    parquet_path = _parquet_out_path(out_path)
    write_transactions_parquet(canonical, parquet_path)
    export.write_dataframe(df, out_path)
    print(f"Wrote canonical parquet to {parquet_path}")
    print(export.wrote_message(out_path, len(df)))


if __name__ == "__main__":
    main()
