import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.cli import _run_build_payee_map
from ynab_il_importer.fingerprint import fingerprint_hash_v1
from ynab_il_importer.rules import PAYEE_MAP_COLUMNS


def test_fingerprint_hash_v1_is_stable() -> None:
    value = fingerprint_hash_v1("expense", "coffee shop")
    assert value == "610547d2f1e0"
    assert value == fingerprint_hash_v1("expense", "coffee shop")
    assert value != fingerprint_hash_v1("transfer", "coffee shop")
    assert len(value) == 12


def test_build_payee_map_outputs_have_bounded_examples_and_no_nan_hints(tmp_path: Path) -> None:
    parsed = pd.DataFrame(
        [
            {
                "txn_kind": "expense",
                "source": "bank",
                "account_name": "A",
                "currency": "ILS",
                "outflow_ils": 20,
                "inflow_ils": 0,
                "description_clean_norm": "local cafe",
                "merchant_raw": "M" * 140,
            },
            {
                "txn_kind": "expense",
                "source": "bank",
                "account_name": "A",
                "currency": "ILS",
                "outflow_ils": 21,
                "inflow_ils": 0,
                "description_clean_norm": "local cafe",
                "merchant_raw": "Second merchant example",
            },
            {
                "txn_kind": "transfer",
                "source": "bank",
                "account_name": "A",
                "currency": "ILS",
                "outflow_ils": 30,
                "inflow_ils": 0,
                "description_clean_norm": "bit transfer",
                "merchant_raw": "BIT",
            },
        ]
    )
    parsed_path = tmp_path / "parsed.csv"
    parsed.to_csv(parsed_path, index=False)

    matched_pairs_path = tmp_path / "matched_pairs.csv"
    pd.DataFrame(
        columns=[
            "account_name",
            "date",
            "outflow_ils",
            "inflow_ils",
            "raw_text",
            "raw_norm",
            "fingerprint_v0",
            "ynab_payee_raw",
            "ynab_category_raw",
        ]
    ).to_csv(matched_pairs_path, index=False)

    map_path = tmp_path / "payee_map.csv"
    pd.DataFrame(columns=PAYEE_MAP_COLUMNS).to_csv(map_path, index=False)

    out_dir = tmp_path / "out"
    candidates, preview = _run_build_payee_map(
        parsed_paths=[parsed_path],
        matched_pairs_paths=[matched_pairs_path],
        out_dir=out_dir,
        map_path=map_path,
    )

    expected_columns = [
        "txn_kind",
        "fingerprint_hash",
        "description_clean_norm",
        "count_in_period",
        "example_1",
        "example_2",
        "suggested_payee_distribution",
        "suggested_category_distribution",
        "existing_rules_hit_count",
        "status",
    ]
    assert candidates.columns.tolist() == expected_columns
    assert "fingerprint_hash" in preview.columns
    assert candidates["suggested_payee_distribution"].isna().sum() == 0
    assert candidates["suggested_category_distribution"].isna().sum() == 0
    assert candidates["example_1"].isna().sum() == 0
    assert candidates["example_2"].isna().sum() == 0
    assert all(len(value) <= 100 for value in candidates["example_1"].tolist())
    assert all(len(value) <= 100 for value in candidates["example_2"].tolist())

    transfer_row = candidates[candidates["txn_kind"] == "transfer"].iloc[0]
    assert transfer_row["example_2"] == ""
