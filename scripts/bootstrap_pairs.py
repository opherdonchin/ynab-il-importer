import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.pairing as pairing


def main() -> None:
    parser = argparse.ArgumentParser(description="Build matched pairs from normalized inputs")
    parser.add_argument("--source", type=Path, action="append", default=[])
    parser.add_argument("--ynab", type=Path, action="append", default=[])
    parser.add_argument("--out", type=Path, default=Path("outputs/matched_pairs.csv"))
    args = parser.parse_args()

    if not args.source or not args.ynab:
        raise SystemExit(
            "Provide at least one --source and --ynab input (repeat flags for multiples)."
        )

    def _load_with_file(paths: list[Path], column_name: str) -> pd.DataFrame:
        frames = []
        for path in paths:
            df = pd.read_csv(path)
            df[column_name] = path.name
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

    source_df = _load_with_file(args.source, "source_file")
    if "source" not in source_df.columns:
        raise ValueError("Source inputs must include a 'source' column.")
    ynab_df = _load_with_file(args.ynab, "ynab_file")

    pairs_df = pairing.match_pairs(source_df, ynab_df)
    export.write_dataframe(pairs_df, args.out)
    print(export.wrote_message(args.out, len(pairs_df)))


if __name__ == "__main__":
    main()
