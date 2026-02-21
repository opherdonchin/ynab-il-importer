import argparse
from pathlib import Path

import pandas as pd


def _most_common_text(series: pd.Series) -> str:
    clean = series.astype("string").fillna("").str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return ""
    return str(clean.value_counts().index[0])


def _top_counts(series: pd.Series, limit: int = 3) -> str:
    clean = series.astype("string").fillna("").str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return ""
    top = clean.value_counts().head(limit)
    return "; ".join(f"{name} ({count})" for name, count in top.items())


def main() -> None:
    parser = argparse.ArgumentParser(description="Build fingerprint groups from matched pairs")
    parser.add_argument("--pairs", type=Path, default=Path("data/derived/matched_pairs.csv"))
    parser.add_argument("--out", type=Path, default=Path("data/derived/fingerprint_groups.csv"))
    args = parser.parse_args()

    pairs = pd.read_csv(args.pairs)
    if "fingerprint_v0" not in pairs.columns:
        raise ValueError("Input pairs file must include fingerprint_v0 column")

    grouped = (
        pairs.groupby("fingerprint_v0", dropna=False)
        .agg(
            count=("fingerprint_v0", "size"),
            example_raw_text=("raw_text", _most_common_text),
            top_ynab_payees=("ynab_payee_raw", _top_counts),
            top_ynab_categories=("ynab_category_raw", _top_counts),
        )
        .reset_index()
    )
    grouped["canonical_payee"] = ""

    args.out.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_csv(args.out, index=False)
    print(f"Wrote {args.out} ({len(grouped)} rows)")


if __name__ == "__main__":
    main()
