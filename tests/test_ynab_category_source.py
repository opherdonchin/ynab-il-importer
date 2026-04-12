from __future__ import annotations

from pathlib import Path

import polars as pl

from ynab_il_importer.artifacts.transaction_io import write_transactions_parquet
import ynab_il_importer.ynab_api as ynab_api
import ynab_il_importer.ynab_category_source as ynab_category_source


def test_build_category_source_canonical_aligns_account_and_preserves_origin(
    tmp_path: Path,
) -> None:
    transactions = [
        {
            "id": "family-parent-1",
            "account_id": "family-acc",
            "date": "2026-04-01",
            "payee_name": "Aikido Dojo",
            "category_name": "Aikido",
            "category_id": "cat-aikido",
            "amount": -350000,
            "memo": "April class",
            "cleared": "cleared",
            "approved": True,
        }
    ]
    accounts = [{"id": "family-acc", "name": "Family Leumi"}]
    source_path = tmp_path / "family_ynab_api_norm.parquet"
    write_transactions_parquet(
        ynab_api.transactions_to_canonical_table(transactions, accounts),
        source_path,
    )

    table = ynab_category_source.build_category_source_canonical(
        source_path,
        category_name="Aikido",
        target_account_name="Personal In Leumi",
        target_account_id="aikido-acc",
        use_fingerprint_map=False,
        fingerprint_map_path=tmp_path / "fingerprint_map.csv",
        fingerprint_log_path=tmp_path / "fingerprint_log.csv",
    )

    df = pl.from_arrow(table)
    row = df.row(0, named=True)
    assert row["source_system"] == "ynab_category"
    assert row["account_name"] == "Personal In Leumi"
    assert row["account_id"] == "aikido-acc"
    assert row["source_account"] == "Family Leumi"
    assert row["category_raw"] == "Aikido"
    assert row["ref"] == "family-parent-1"
    assert row["fingerprint"] == "aikido dojo"


def test_build_category_source_canonical_flattens_matching_split_rows(
    tmp_path: Path,
) -> None:
    transactions = [
        {
            "id": "family-parent-1",
            "account_id": "family-acc",
            "date": "2026-04-01",
            "payee_name": "Salary Liya",
            "category_name": "Split",
            "category_id": "",
            "amount": 0,
            "memo": "",
            "cleared": "cleared",
            "approved": True,
            "subtransactions": [
                {
                    "id": "split-aikido",
                    "amount": -350000,
                    "memo": "April class",
                    "payee_name": "Aikido Dojo",
                    "category_id": "cat-aikido",
                    "category_name": "Aikido",
                    "deleted": False,
                },
                {
                    "id": "split-rta",
                    "amount": 350000,
                    "memo": "",
                    "payee_name": "",
                    "category_id": "cat-rta",
                    "category_name": "Inflow: Ready to Assign",
                    "deleted": False,
                },
            ],
        }
    ]
    accounts = [{"id": "family-acc", "name": "Family Leumi"}]
    source_path = tmp_path / "family_ynab_api_norm.parquet"
    write_transactions_parquet(
        ynab_api.transactions_to_canonical_table(transactions, accounts),
        source_path,
    )

    table = ynab_category_source.build_category_source_canonical(
        source_path,
        category_id="cat-aikido",
        target_account_name="Personal In Leumi",
        use_fingerprint_map=False,
        fingerprint_map_path=tmp_path / "fingerprint_map.csv",
        fingerprint_log_path=tmp_path / "fingerprint_log.csv",
    )

    df = pl.from_arrow(table)
    row = df.row(0, named=True)
    assert row["transaction_id"] == "split-aikido"
    assert row["ynab_id"] == "split-aikido"
    assert row["is_subtransaction"] is True
    assert row["ref"] == "family-parent-1"
    assert row["source_account"] == "Family Leumi"
