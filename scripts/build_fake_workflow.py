import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.io_ynab as ynab


def _load_csvs(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        df = pd.read_csv(path)
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build fake sources + fake YNAB datasets")
    parser.add_argument("--sources", action="append", required=True)
    parser.add_argument("--ynab", required=True)
    parser.add_argument("--pairs", required=True)
    parser.add_argument("--out-dir", default="data/fake")
    parser.add_argument("--dupes", type=int, default=100)
    parser.add_argument("--extra-sources", type=int, default=200)
    parser.add_argument("--extra-ynab", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    source_df = _load_csvs([Path(p) for p in args.sources])
    if source_df.empty:
        raise ValueError("No source rows found.")
    ynab_df = pd.read_csv(Path(args.ynab))
    pairs_df = pd.read_csv(Path(args.pairs))

    key_cols = ["account_name", "date", "outflow_ils", "inflow_ils"]
    dupe_keys = pairs_df[key_cols].drop_duplicates()
    source_dupes = source_df.merge(dupe_keys, on=key_cols, how="inner")
    source_dupes = source_dupes.sample(
        n=min(args.dupes, len(source_dupes)), random_state=args.seed
    )

    remaining = source_df.merge(dupe_keys, on=key_cols, how="left", indicator=True)
    remaining = remaining[remaining["_merge"] == "left_only"].drop(columns=["_merge"])
    extra_sources = remaining.sample(
        n=min(args.extra_sources, len(remaining)), random_state=args.seed
    )

    fake_source = pd.concat([source_dupes, extra_sources], ignore_index=True)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if "source" in fake_source.columns:
        fake_bank = fake_source[fake_source["source"].str.lower() == "bank"].copy()
        fake_card = fake_source[fake_source["source"].str.lower() == "card"].copy()
    else:
        fake_bank = fake_source.copy()
        fake_card = fake_source.iloc[0:0].copy()

    bank_path = out_dir / "fake_bank_norm.csv"
    card_path = out_dir / "fake_card_norm.csv"
    export.write_dataframe(fake_bank, bank_path)
    export.write_dataframe(fake_card, card_path)

    # Build fake YNAB data: use matched YNAB rows for dupes + extra random YNAB rows
    dupe_ynab = pairs_df.merge(source_dupes[key_cols], on=key_cols, how="inner")
    ynab_rows = pd.DataFrame(
        {
            "source": "ynab",
            "account_name": dupe_ynab["account_name"],
            "date": dupe_ynab["date"],
            "payee_raw": dupe_ynab["ynab_payee_raw"],
            "category_raw": dupe_ynab["ynab_category_raw"],
            "outflow_ils": dupe_ynab["ynab_outflow_ils"],
            "inflow_ils": dupe_ynab["ynab_inflow_ils"],
            "memo": "",
            "currency": "ILS",
            "amount_bucket": "",
        }
    )

    extra_ynab = ynab_df.sample(
        n=min(args.extra_ynab, len(ynab_df)), random_state=args.seed
    )
    extra_ynab = extra_ynab[
        [
            "source",
            "account_name",
            "date",
            "payee_raw",
            "category_raw",
            "outflow_ils",
            "inflow_ils",
            "memo",
            "currency",
            "amount_bucket",
        ]
    ].copy()

    fake_ynab = pd.concat([ynab_rows, extra_ynab], ignore_index=True)
    fake_ynab["txn_kind"] = ynab._infer_txn_kind(
        fake_ynab["inflow_ils"],
        fake_ynab["outflow_ils"],
        fake_ynab["payee_raw"],
        fake_ynab["category_raw"],
    )

    ynab_path = out_dir / "fake_ynab_norm.csv"
    export.write_dataframe(fake_ynab, ynab_path)

    print(export.wrote_message(bank_path, len(fake_bank)))
    print(export.wrote_message(card_path, len(fake_card)))
    print(export.wrote_message(ynab_path, len(fake_ynab)))


if __name__ == "__main__":
    main()
