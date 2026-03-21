import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.workflow_profiles as workflow_profiles
import ynab_il_importer.ynab_api as ynab_api


def main() -> None:
    parser = argparse.ArgumentParser(description="Download YNAB categories via API")
    parser.add_argument("--profile", default="", help="Workflow profile (for budget/category defaults).")
    parser.add_argument("--budget-id", dest="budget_id", default="", help="Override YNAB budget/plan id.")
    parser.add_argument(
        "--out",
        dest="out_path",
        type=Path,
        default=None,
    )
    args = parser.parse_args()

    profile = workflow_profiles.resolve_profile(args.profile or None)
    plan_id = workflow_profiles.resolve_budget_id(
        profile=profile.name,
        budget_id=args.budget_id,
    )
    out_path = args.out_path or profile.categories_path

    groups = ynab_api.fetch_categories(plan_id=plan_id or None)
    df = ynab_api.categories_to_dataframe(groups)
    if df.empty:
        raise ValueError("No categories returned from YNAB API.")

    export.write_dataframe(df, out_path)
    print(export.wrote_message(out_path, len(df)))


if __name__ == "__main__":
    main()
