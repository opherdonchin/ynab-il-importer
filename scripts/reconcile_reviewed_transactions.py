import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_reconcile as review_reconcile


def main() -> None:
    parser = argparse.ArgumentParser(description="Port reviewed decisions onto a rebuilt proposed CSV")
    parser.add_argument("--old-reviewed", required=True, dest="old_reviewed")
    parser.add_argument("--new-proposed", required=True, dest="new_proposed")
    parser.add_argument("--out", required=True, dest="out_path")
    args = parser.parse_args()

    old_df = review_io.load_proposed_transactions(Path(args.old_reviewed))
    new_df = review_io.load_proposed_transactions(Path(args.new_proposed))
    merged, stats = review_reconcile.reconcile_reviewed_transactions(old_df, new_df)
    review_io.save_reviewed_transactions(merged, Path(args.out_path))

    print(f"Wrote {args.out_path} ({len(merged)} rows)")
    print(
        "Reconciled decisions: "
        f"direct={stats['direct_matches']}, "
        f"fallback={stats['fallback_matches']}, "
        f"untouched={stats['untouched_rows']}"
    )


if __name__ == "__main__":
    main()
