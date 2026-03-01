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
    parser.add_argument("--bank", type=Path, action="append", default=[])
    parser.add_argument("--card", type=Path, action="append", default=[])
    parser.add_argument("--ynab", type=Path, action="append", default=[])
    parser.add_argument("--out", type=Path, default=Path("outputs/matched_pairs.csv"))
    args = parser.parse_args()

    if not args.bank or not args.card or not args.ynab:
        raise SystemExit(
            "Provide at least one --bank, --card, and --ynab input (repeat flags for multiples)."
        )

    def _load_with_file(paths: list[Path], column_name: str) -> pd.DataFrame:
        frames = []
        for path in paths:
            df = pd.read_csv(path)
            df[column_name] = path.name
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

    bank_df = _load_with_file(args.bank, "source_file")
    card_df = _load_with_file(args.card, "source_file")
    ynab_df = _load_with_file(args.ynab, "ynab_file")

    pairs_df = match_pairs(bank_df, card_df, ynab_df)
    write_dataframe(pairs_df, args.out)
    print(f"Wrote {args.out} ({len(pairs_df)} rows)")


if __name__ == "__main__":
    main()
