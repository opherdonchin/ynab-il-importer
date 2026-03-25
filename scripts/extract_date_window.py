import argparse
from datetime import timedelta
from pathlib import Path

import pandas as pd


def _load_dates(path: Path, date_column: str) -> list[pd.Timestamp]:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_csv(path)
    if date_column not in df.columns:
        raise ValueError(f"{path} is missing required date column: {date_column}")
    parsed = pd.to_datetime(df[date_column], errors="coerce").dropna()
    return list(parsed)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Extract min/max source dates from normalized CSV files and emit "
            "bounded YNAB download window suggestions."
        )
    )
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        help="Input CSV path. Repeat --source for multiple files.",
    )
    parser.add_argument(
        "--date-column",
        default="date",
        help="Date column name in the input CSV files (default: date).",
    )
    parser.add_argument(
        "--padding-days",
        type=int,
        default=14,
        help="Days to subtract/add around source min/max (default: 14).",
    )
    parser.add_argument(
        "--label",
        default="window",
        help="Prefix for printed keys (default: window).",
    )
    parser.add_argument(
        "--print-args",
        action="store_true",
        help="Also print a ready-to-paste CLI fragment: --since ... --until ...",
    )
    args = parser.parse_args()

    all_dates: list[pd.Timestamp] = []
    for source in args.source:
        all_dates.extend(_load_dates(Path(source), args.date_column))

    if not all_dates:
        raise ValueError("No parseable dates found across input sources.")

    min_date = min(all_dates).date()
    max_date = max(all_dates).date()
    since_date = min_date - timedelta(days=args.padding_days)
    until_date = max_date + timedelta(days=args.padding_days)
    prefix = args.label.strip() or "window"

    print(f"{prefix}_source_min_date={min_date.isoformat()}")
    print(f"{prefix}_source_max_date={max_date.isoformat()}")
    print(f"{prefix}_ynab_since={since_date.isoformat()}")
    print(f"{prefix}_ynab_until={until_date.isoformat()}")
    if args.print_args:
        print(f"--since {since_date.isoformat()} --until {until_date.isoformat()}")


if __name__ == "__main__":
    main()
