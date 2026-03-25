import argparse
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a ynab_categories.csv-style file from a YNAB transaction snapshot "
            "(for budgets where categories API is empty)."
        )
    )
    parser.add_argument("--ynab", required=True, help="YNAB snapshot CSV path (for example: ynab_api_norm.csv).")
    parser.add_argument("--out", required=True, help="Output categories CSV path.")
    parser.add_argument(
        "--group-name",
        default="Imported from ynab_api_norm",
        help="Category group label to assign in the generated file.",
    )
    args = parser.parse_args()

    ynab_path = Path(args.ynab)
    out_path = Path(args.out)
    df = pd.read_csv(ynab_path)
    if "category_raw" not in df.columns:
        raise ValueError(f"{ynab_path} is missing required column: category_raw")

    categories = (
        df["category_raw"]
        .astype("string")
        .fillna("")
        .str.strip()
    )
    categories = categories[categories != ""]
    categories = categories.drop_duplicates().sort_values().reset_index(drop=True)

    out = pd.DataFrame(
        {
            "category_group": [str(args.group_name).strip() or "Imported from ynab_api_norm"] * len(categories),
            "category_group_id": [""] * len(categories),
            "category_name": categories,
            "category_id": [""] * len(categories),
            "hidden": ["False"] * len(categories),
        }
    )

    export.write_dataframe(out, out_path)
    print(export.wrote_message(out_path, len(out)))


if __name__ == "__main__":
    main()
