import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.export import write_dataframe
from ynab_il_importer.ynab_api import (
    fetch_accounts,
    fetch_transactions,
    transactions_to_dataframe,
)


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Download YNAB transactions via API")
    parser.add_argument("--since", dest="since_date", default="", help="YYYY-MM-DD")
    parser.add_argument("--until", dest="until_date", default="", help="YYYY-MM-DD")
    parser.add_argument(
        "--out",
        dest="out_path",
        type=Path,
        default=Path("data/derived/ynab_api_norm.csv"),
    )
    args = parser.parse_args()

    accounts = fetch_accounts()
    txns = fetch_transactions(since_date=args.since_date or None)
    df = transactions_to_dataframe(txns, accounts)
    df = _filter_by_date(df, args.since_date or None, args.until_date or None)

    write_dataframe(df, args.out_path)
    print(f"Wrote {args.out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
