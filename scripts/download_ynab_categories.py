import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.ynab_api as ynab_api


def main() -> None:
    parser = argparse.ArgumentParser(description="Download YNAB categories via API")
    parser.add_argument(
        "--out",
        dest="out_path",
        type=Path,
        default=Path("outputs/ynab_categories.csv"),
    )
    args = parser.parse_args()

    groups = ynab_api.fetch_categories()
    df = ynab_api.categories_to_dataframe(groups)
    if df.empty:
        raise ValueError("No categories returned from YNAB API.")

    export.write_dataframe(df, args.out_path)
    print(f"Wrote {args.out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
