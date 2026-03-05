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


def _unique_values(series: pd.Series) -> str:
    clean = series.astype("string").fillna("").str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return ""
    ordered = clean.value_counts().index.tolist()
    return "; ".join(str(value) for value in ordered)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build fingerprint groups from matched pairs")
    parser.add_argument("--pairs", type=Path, default=Path("outputs/matched_pairs.csv"))
    parser.add_argument("--out", type=Path, default=Path("outputs/fingerprint_groups.csv"))
    args = parser.parse_args()

    pairs = pd.read_csv(args.pairs)
    if "fingerprint" not in pairs.columns:
        raise ValueError("Input pairs file must include fingerprint column")
    fingerprint_col = "fingerprint"
    if "ynab_payee_raw" not in pairs.columns:
        raise ValueError("Input pairs file must include ynab_payee_raw column")

    grouped = (
        pairs.groupby(fingerprint_col, dropna=False)
        .agg(
            count=(fingerprint_col, "size"),
            example_raw_text=("raw_text", _most_common_text),
            top_ynab_payees=("ynab_payee_raw", _top_counts),
            top_ynab_categories=("ynab_category_raw", _top_counts),
            canonical_payee=("ynab_payee_raw", _unique_values),
        )
        .reset_index()
        .rename(columns={fingerprint_col: "fingerprint"})
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"Wrote {args.out} ({len(grouped)} rows)")


if __name__ == "__main__":
    main()
