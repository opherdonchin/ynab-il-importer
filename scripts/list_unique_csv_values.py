import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print sorted unique values for a specific CSV column."
    )
    parser.add_argument("--csv", dest="csv_path", required=True, help="Input CSV path.")
    parser.add_argument("--column", required=True, help="Column name to list unique values from.")
    parser.add_argument(
        "--drop-empty",
        action="store_true",
        help="Drop empty-string values from the output.",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"File not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if args.column not in df.columns:
        raise ValueError(f"{csv_path} is missing requested column: {args.column}")

    series = df[args.column].astype("string").fillna("")
    values = sorted(series.unique())
    if args.drop_empty:
        values = [value for value in values if value.strip()]

    for value in values:
        print(value)


if __name__ == "__main__":
    main()
