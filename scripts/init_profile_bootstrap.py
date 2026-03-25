import argparse
import shutil
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.rules as rules


def _write_account_map(
    path: Path,
    *,
    source: str,
    source_account: str,
    ynab_account_name: str,
    ynab_account_id: str,
) -> None:
    df = pd.DataFrame(
        [
            {
                "source": source,
                "source_account": source_account,
                "source_account_label": "",
                "ynab_account_name": ynab_account_name,
                "ynab_account_id": ynab_account_id,
            }
        ]
    )
    export.write_dataframe(df, path)
    print(export.wrote_message(path, len(df)))


def _write_empty_payee_map(path: Path) -> None:
    df = pd.DataFrame(columns=list(rules.PAYEE_MAP_COLUMNS))
    export.write_dataframe(df, path)
    print(export.wrote_message(path, len(df)))


def _write_empty_categories(path: Path) -> None:
    df = pd.DataFrame(
        columns=[
            "category_group",
            "category_group_id",
            "category_name",
            "category_id",
            "hidden",
        ]
    )
    export.write_dataframe(df, path)
    print(export.wrote_message(path, len(df)))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialize profile-scoped mapping/output bootstrap files."
    )
    parser.add_argument("--profile", required=True, help="Profile name (for example: aikido).")
    parser.add_argument("--account-name", required=True, help="Target YNAB account name.")
    parser.add_argument("--account-id", required=True, help="Target YNAB account id.")
    parser.add_argument(
        "--source",
        default="ynab",
        help="Source label for account_name_map.csv (default: ynab).",
    )
    parser.add_argument(
        "--source-account",
        default="",
        help="Source account text in account_name_map.csv (default: same as --account-name).",
    )
    parser.add_argument(
        "--base-fingerprint-map",
        default="mappings/fingerprint_map.csv",
        help="Fingerprint map to copy as profile baseline.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing profile files.",
    )
    args = parser.parse_args()

    profile = str(args.profile).strip().lower()
    if not profile:
        raise ValueError("--profile cannot be empty.")

    mappings_dir = Path("mappings") / profile
    outputs_dir = Path("outputs") / profile
    derived_dir = Path("data/derived") / profile
    paired_dir = Path("data/paired") / profile
    for path in [mappings_dir, outputs_dir, derived_dir, paired_dir]:
        path.mkdir(parents=True, exist_ok=True)
        print(f"Ensured {path.as_posix()}")

    account_map_path = mappings_dir / "account_name_map.csv"
    fingerprint_map_path = mappings_dir / "fingerprint_map.csv"
    payee_map_path = mappings_dir / "payee_map.csv"
    categories_path = outputs_dir / "ynab_categories.csv"

    if args.overwrite or not account_map_path.exists():
        _write_account_map(
            account_map_path,
            source=str(args.source).strip() or "ynab",
            source_account=(str(args.source_account).strip() or str(args.account_name).strip()),
            ynab_account_name=str(args.account_name).strip(),
            ynab_account_id=str(args.account_id).strip(),
        )
    else:
        print(f"Keeping existing {account_map_path.as_posix()}")

    base_fingerprint_map = Path(args.base_fingerprint_map)
    if args.overwrite or not fingerprint_map_path.exists():
        if not base_fingerprint_map.exists():
            raise ValueError(f"Base fingerprint map not found: {base_fingerprint_map}")
        shutil.copyfile(base_fingerprint_map, fingerprint_map_path)
        print(f"Copied {base_fingerprint_map.as_posix()} -> {fingerprint_map_path.as_posix()}")
    else:
        print(f"Keeping existing {fingerprint_map_path.as_posix()}")

    if args.overwrite or not payee_map_path.exists():
        _write_empty_payee_map(payee_map_path)
    else:
        print(f"Keeping existing {payee_map_path.as_posix()}")

    if args.overwrite or not categories_path.exists():
        _write_empty_categories(categories_path)
    else:
        print(f"Keeping existing {categories_path.as_posix()}")


if __name__ == "__main__":
    main()
