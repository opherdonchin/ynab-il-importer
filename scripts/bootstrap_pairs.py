import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.export import write_dataframe
from ynab_il_importer.pairing import match_pairs


def main() -> None:
    parser = argparse.ArgumentParser(description="Build matched pairs from normalized inputs")
    parser.add_argument("--bank", type=Path, default=Path("data/derived/bank_normalized.csv"))
    parser.add_argument("--card", type=Path, default=Path("data/derived/card_normalized.csv"))
    parser.add_argument("--ynab", type=Path, default=Path("data/derived/ynab_normalized.csv"))
    parser.add_argument("--out", type=Path, default=Path("data/derived/matched_pairs.csv"))
    args = parser.parse_args()

    bank_df = pd.read_csv(args.bank)
    card_df = pd.read_csv(args.card)
    ynab_df = pd.read_csv(args.ynab)

    pairs_df = match_pairs(bank_df, card_df, ynab_df)
    write_dataframe(pairs_df, args.out)
    print(f"Wrote {args.out} ({len(pairs_df)} rows)")


if __name__ == "__main__":
    main()
