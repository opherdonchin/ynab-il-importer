import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.review_app.io as review_io


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Translate a legacy review CSV into the unified review-row schema."
    )
    parser.add_argument("--in", dest="in_path", required=True, help="Input review CSV path.")
    parser.add_argument("--out", dest="out_path", required=True, help="Output unified review CSV path.")
    args = parser.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    df = pd.read_csv(in_path, dtype="string").fillna("")
    detected_format = review_io.detect_review_csv_format(df)
    if detected_format == "unified_v1":
        translated = df.copy()
    elif detected_format.startswith("legacy_"):
        translated = review_io.translate_review_dataframe(df)
    else:
        raise ValueError(f"Unsupported review CSV format: {detected_format}")

    review_io.save_reviewed_transactions(translated, out_path)
    print(export.wrote_message(out_path, len(translated)))
    print(f"Translated review format: {detected_format} -> unified_v1")


if __name__ == "__main__":
    main()
