import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.rules import PAYEE_MAP_COLUMNS, load_payee_map


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate payee_map.csv")
    parser.add_argument("--map", dest="map_path", type=Path, default=Path("mappings/payee_map.csv"))
    args = parser.parse_args()

    if not args.map_path.exists():
        raise FileNotFoundError(f"Missing payee map: {args.map_path}")

    raw = pd.read_csv(args.map_path, dtype="string").fillna("")
    missing = [col for col in PAYEE_MAP_COLUMNS if col not in raw.columns]
    if missing:
        raise ValueError(f"payee_map missing columns: {missing}")

    if raw["payee_canonical"].astype("string").str.contains(";", regex=False).any():
        raise ValueError("payee_map has ';' in payee_canonical")
    if raw["category_target"].astype("string").str.contains(";", regex=False).any():
        raise ValueError("payee_map has ';' in category_target")

    normalized = load_payee_map(args.map_path)
    if (normalized["fingerprint"].astype("string").fillna("").str.strip() == "").any():
        print("Warning: payee_map contains empty fingerprint values.")

    print(f"Validated {len(raw)} rows in {args.map_path}")


if __name__ == "__main__":
    main()
