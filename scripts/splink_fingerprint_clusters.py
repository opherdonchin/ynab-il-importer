import argparse
from pathlib import Path

import pandas as pd
from splink.backends.duckdb import DuckDBAPI
from splink.blocking_rule_library import block_on
from splink.comparison_library import JaroWinklerAtThresholds
from splink.internals.linker import Linker
from splink.internals.settings_creator import SettingsCreator


def _first_token(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    return value.split()[0]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cluster similar fingerprints using Splink to suggest consolidation rules."
    )
    parser.add_argument(
        "--in",
        dest="input_path",
        type=Path,
        default=Path("outputs/fingerprint_groups.csv"),
    )
    parser.add_argument(
        "--out",
        dest="output_path",
        type=Path,
        default=Path("outputs/splink_fingerprint_clusters.csv"),
    )
    parser.add_argument(
        "--threshold",
        dest="threshold",
        type=float,
        default=0.95,
        help="Clustering match probability threshold.",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.input_path).fillna("")
    if "fingerprint" not in df.columns:
        raise ValueError("Input file must include fingerprint column.")

    df = df.copy()
    df["unique_id"] = df.index.astype(int)
    df["fp_prefix"] = df["fingerprint"].map(_first_token)
    df["fp_prefix4"] = df["fingerprint"].astype(str).str[:4]

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            JaroWinklerAtThresholds("fingerprint", [0.9, 0.95]),
            JaroWinklerAtThresholds("example_raw_text", [0.9]),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("fp_prefix"),
            block_on("fp_prefix4"),
        ],
        additional_columns_to_retain=[
            "fingerprint",
            "example_raw_text",
            "top_ynab_payees",
            "top_ynab_categories",
            "canonical_payee",
        ],
    )

    linker = Linker(df, settings, DuckDBAPI())
    predictions = linker.inference.predict()
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions, threshold_match_probability=args.threshold
    )

    out_path = args.output_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    clusters.as_pandas_dataframe().to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
