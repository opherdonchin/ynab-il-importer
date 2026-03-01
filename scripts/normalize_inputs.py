import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.export import write_dataframe
from ynab_il_importer.io_bank import read_bank
from ynab_il_importer.io_card import read_card
from ynab_il_importer.io_ynab import read_ynab_register


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize bank/card/YNAB raw inputs")
    parser.add_argument("--bank-in", type=Path, required=False)
    parser.add_argument("--card-in", type=Path, required=False)
    parser.add_argument("--ynab-in", type=Path, required=False)
    parser.add_argument("--out-dir", type=Path, default=Path("data/derived"))
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    bank_df = read_bank(args.bank_in)
    card_df = read_card(args.card_in)
    ynab_df = read_ynab_register(args.ynab_in)

    bank_out = args.out_dir / "bank_normalized.csv"
    card_out = args.out_dir / "card_normalized.csv"
    ynab_out = args.out_dir / "ynab_normalized.csv"

    write_dataframe(bank_df, bank_out)
    write_dataframe(card_df, card_out)
    write_dataframe(ynab_df, ynab_out)

    print(f"Wrote {bank_out} ({len(bank_df)} rows)")
    print(f"Wrote {card_out} ({len(card_df)} rows)")
    print(f"Wrote {ynab_out} ({len(ynab_df)} rows)")


if __name__ == "__main__":
    main()
