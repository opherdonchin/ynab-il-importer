import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.cross_budget_pairing as cross_budget_pairing
import ynab_il_importer.export as export


def _filter_by_date(df: pd.DataFrame, since: str | None, until: str | None) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    date_series = pd.to_datetime(out["date"], errors="coerce")
    if since:
        out = out.loc[date_series >= pd.to_datetime(since, errors="coerce")].copy()
        date_series = pd.to_datetime(out["date"], errors="coerce")
    if until:
        out = out.loc[date_series <= pd.to_datetime(until, errors="coerce")].copy()
    return out


def _default_artifact_root(target_profile: str, phase: str) -> Path:
    profile_name = str(target_profile or "").strip().lower()
    if profile_name:
        return Path("data/paired") / f"{profile_name}_cross_budget_{phase}"
    return Path("data/paired") / f"cross_budget_{phase}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build matched and unresolved cross-budget artifacts from source and target CSVs."
    )
    parser.add_argument("--source", required=True, help="Source CSV path.")
    parser.add_argument("--ynab", required=True, help="Target YNAB snapshot CSV path.")
    parser.add_argument("--source-profile", default="", help="Source workflow profile (for artifact naming only).")
    parser.add_argument("--source-category", default="", help="Optional source category filter.")
    parser.add_argument("--target-profile", default="", help="Target workflow profile (for artifact naming only).")
    parser.add_argument("--target-account", required=True, help="Target account name in the target budget.")
    parser.add_argument("--since", default="", help="Start date YYYY-MM-DD.")
    parser.add_argument("--until", default="", help="End date YYYY-MM-DD.")
    parser.add_argument(
        "--date-tolerance-days",
        type=int,
        default=1,
        help="Allow date-window matching within this many days after exact matching fails.",
    )
    parser.add_argument("--pairs-out", default="", help="Matched-pairs output CSV.")
    parser.add_argument(
        "--unmatched-source-out",
        default="",
        help="Unmatched source rows output CSV.",
    )
    parser.add_argument(
        "--unmatched-target-out",
        default="",
        help="Unmatched target rows output CSV.",
    )
    parser.add_argument(
        "--ambiguous-out",
        default="",
        help="Ambiguous-match output CSV.",
    )
    args = parser.parse_args()

    source_path = Path(args.source)
    target_path = Path(args.ynab)
    source_df = pd.read_csv(source_path).fillna("")
    target_df = pd.read_csv(target_path).fillna("")
    source_df["source_file"] = source_path.name
    target_df["target_file"] = target_path.name

    source_df = _filter_by_date(source_df, args.since or None, args.until or None)
    target_df = _filter_by_date(target_df, args.since or None, args.until or None)

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account=args.target_account,
        source_category=args.source_category or None,
        date_tolerance_days=int(args.date_tolerance_days),
    )

    artifact_root = _default_artifact_root(args.target_profile, "bootstrap")
    pairs_out = Path(args.pairs_out) if args.pairs_out else artifact_root / "matched_pairs.csv"
    unmatched_source_out = (
        Path(args.unmatched_source_out)
        if args.unmatched_source_out
        else artifact_root / "unmatched_source.csv"
    )
    unmatched_target_out = (
        Path(args.unmatched_target_out)
        if args.unmatched_target_out
        else artifact_root / "unmatched_target.csv"
    )
    ambiguous_out = (
        Path(args.ambiguous_out)
        if args.ambiguous_out
        else artifact_root / "ambiguous_matches.csv"
    )

    export.write_dataframe(result.matched_pairs_df, pairs_out)
    export.write_dataframe(result.unmatched_source_df, unmatched_source_out)
    export.write_dataframe(result.unmatched_target_df, unmatched_target_out)
    export.write_dataframe(result.ambiguous_matches_df, ambiguous_out)

    print(export.wrote_message(pairs_out, len(result.matched_pairs_df)))
    print(export.wrote_message(unmatched_source_out, len(result.unmatched_source_df)))
    print(export.wrote_message(unmatched_target_out, len(result.unmatched_target_df)))
    print(export.wrote_message(ambiguous_out, len(result.ambiguous_matches_df)))
    print(
        "Summary: "
        f"matched={len(result.matched_pairs_df)} "
        f"unmatched_source={len(result.unmatched_source_df)} "
        f"unmatched_target={len(result.unmatched_target_df)} "
        f"ambiguous={len(result.ambiguous_matches_df)}"
    )


if __name__ == "__main__":
    main()
