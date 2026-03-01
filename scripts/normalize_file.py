import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.export import write_dataframe
from ynab_il_importer.io_bankin import read_bankin_dat
from ynab_il_importer.io_card import read_card
from ynab_il_importer.io_ynab import read_ynab_register


FORMAT_READERS = {
    "leumi": read_bankin_dat,
    "max": read_card,
    "ynab": read_ynab_register,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize a single input file")
    parser.add_argument("--in", dest="in_path", type=Path, required=True)
    parser.add_argument("--out", dest="out_path", type=Path, required=True)
    parser.add_argument(
        "--format",
        dest="format_name",
        choices=sorted(FORMAT_READERS.keys()),
        required=True,
    )
    args = parser.parse_args()

    reader = FORMAT_READERS[args.format_name]
    df = reader(args.in_path)

    args.out_path.parent.mkdir(parents=True, exist_ok=True)
    write_dataframe(df, args.out_path)
    print(f"Wrote {args.out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
