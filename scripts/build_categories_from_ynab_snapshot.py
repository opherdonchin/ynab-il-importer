import argparse
import io
from pathlib import Path
import sys
import zipfile

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export


def _read_csv_or_zip_member(path: Path, *, suffix: str) -> pd.DataFrame:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            members = [name for name in archive.namelist() if name.lower().endswith(suffix.lower())]
            if not members:
                raise ValueError(f"Could not find {suffix} inside {path}")
            if len(members) > 1:
                raise ValueError(f"Multiple {suffix} files found inside {path}: {members}")
            raw = archive.read(members[0]).decode("utf-8-sig")
        return pd.read_csv(io.StringIO(raw))
    return pd.read_csv(path)


def _categories_from_plan(plan_df: pd.DataFrame) -> pd.DataFrame:
    if "Category Group" not in plan_df.columns or "Category" not in plan_df.columns:
        raise ValueError("Plan.csv is missing required columns: 'Category Group' and 'Category'")

    categories = (
        plan_df.loc[:, ["Category Group", "Category"]]
        .rename(columns={"Category Group": "category_group", "Category": "category_name"})
        .astype("string")
        .fillna("")
    )
    categories["category_group"] = categories["category_group"].str.strip()
    categories["category_name"] = categories["category_name"].str.strip()
    categories = categories.loc[categories["category_name"] != ""].drop_duplicates()
    categories = categories.sort_values(["category_group", "category_name"]).reset_index(drop=True)

    out = pd.DataFrame(
        {
            "category_group": categories["category_group"],
            "category_group_id": [""] * len(categories),
            "category_name": categories["category_name"],
            "category_id": [""] * len(categories),
            "hidden": ["False"] * len(categories),
        }
    )
    return out


def _categories_from_snapshot(snapshot_df: pd.DataFrame, *, group_name: str) -> pd.DataFrame:
    if "category_raw" not in snapshot_df.columns:
        raise ValueError("Snapshot input is missing required column: category_raw")

    categories = (
        snapshot_df["category_raw"]
        .astype("string")
        .fillna("")
        .str.strip()
    )
    categories = categories[categories != ""]
    categories = categories.drop_duplicates().sort_values().reset_index(drop=True)

    return pd.DataFrame(
        {
            "category_group": [group_name] * len(categories),
            "category_group_id": [""] * len(categories),
            "category_name": categories,
            "category_id": [""] * len(categories),
            "hidden": ["False"] * len(categories),
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a ynab_categories.csv-style file from a YNAB export Plan.csv, "
            "a YNAB export zip, or a normalized transaction snapshot."
        )
    )
    parser.add_argument(
        "--ynab",
        required=True,
        help=(
            "YNAB source path. Accepts a normalized snapshot CSV, a Plan.csv export, "
            "or a YNAB export zip containing Plan.csv."
        ),
    )
    parser.add_argument("--out", required=True, help="Output categories CSV path.")
    parser.add_argument(
        "--group-name",
        default="Imported from ynab_api_norm",
        help="Category group label to assign in the generated file.",
    )
    args = parser.parse_args()

    ynab_path = Path(args.ynab)
    out_path = Path(args.out)
    group_name = str(args.group_name).strip() or "Imported from ynab_api_norm"

    if ynab_path.suffix.lower() == ".zip" or ynab_path.name.lower().endswith("plan.csv"):
        out = _categories_from_plan(_read_csv_or_zip_member(ynab_path, suffix="Plan.csv"))
    else:
        df = pd.read_csv(ynab_path)
        if "Category Group" in df.columns and "Category" in df.columns:
            out = _categories_from_plan(df)
        else:
            out = _categories_from_snapshot(df, group_name=group_name)

    export.write_dataframe(out, out_path)
    print(export.wrote_message(out_path, len(out)))


if __name__ == "__main__":
    main()
