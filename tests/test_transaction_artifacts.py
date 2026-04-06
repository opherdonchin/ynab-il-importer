from __future__ import annotations

import polars as pl
import pyarrow as pa

from ynab_il_importer.artifacts.transaction_io import (
    flat_projection_to_canonical_table,
    load_flat_transaction_projection,
    normalize_transaction_table,
    read_transactions_arrow,
    read_transactions_polars,
    write_flat_transaction_artifacts,
    write_transactions_parquet,
)
from ynab_il_importer.artifacts.transaction_projection import (
    explode_split_lines,
    project_top_level_transactions,
)
from ynab_il_importer.artifacts.transaction_schema import TRANSACTION_SCHEMA


def _sample_transaction_table() -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "artifact_kind": "ynab_transaction",
                "artifact_version": "transaction_v1",
                "source_system": "ynab",
                "transaction_id": "txn-1",
                "account_name": "Family",
                "date": "2026-03-28",
                "inflow_ils": 0.0,
                "outflow_ils": 120.0,
                "signed_amount_ils": -120.0,
                "payee_raw": "Tsomet Sfarim",
                "category_raw": "Split",
                "memo": "Parent",
                "txn_kind": "expense",
                "fingerprint": "tsomet_sfarim",
                "approved": True,
                "is_subtransaction": False,
                "splits": [
                    {
                        "split_id": "split-1",
                        "parent_transaction_id": "txn-1",
                        "ynab_subtransaction_id": "sub-1",
                        "payee_raw": "Tsomet Sfarim",
                        "category_id": "cat-books",
                        "category_raw": "Books",
                        "memo": "Books line",
                        "inflow_ils": 0.0,
                        "outflow_ils": 80.0,
                        "import_id": "import-1",
                        "matched_transaction_id": "",
                    },
                    {
                        "split_id": "split-2",
                        "parent_transaction_id": "txn-1",
                        "ynab_subtransaction_id": "sub-2",
                        "payee_raw": "Tsomet Sfarim",
                        "category_id": "cat-gifts",
                        "category_raw": "Gifts",
                        "memo": "Gift line",
                        "inflow_ils": 0.0,
                        "outflow_ils": 40.0,
                        "import_id": "import-2",
                        "matched_transaction_id": "",
                    },
                ],
            }
        ],
        schema=TRANSACTION_SCHEMA,
    )


def test_normalize_transaction_table_adds_missing_schema_columns() -> None:
    table = pa.table({"transaction_id": ["txn-1"], "account_name": ["Family"]})

    normalized = normalize_transaction_table(table)

    assert normalized.schema == TRANSACTION_SCHEMA
    assert normalized["transaction_id"].to_pylist() == ["txn-1"]
    assert normalized["account_name"].to_pylist() == ["Family"]
    assert normalized["splits"].to_pylist() == [None]


def test_transaction_parquet_round_trip_preserves_split_structure(tmp_path) -> None:
    path = tmp_path / "transactions.parquet"
    table = _sample_transaction_table()

    write_transactions_parquet(table, path)
    loaded_arrow = read_transactions_arrow(path)
    loaded_polars = read_transactions_polars(path)

    assert loaded_arrow["splits"].to_pylist()[0][0]["category_raw"] == "Books"
    assert isinstance(loaded_polars, pl.DataFrame)
    assert loaded_polars.height == 1
    assert loaded_arrow.to_pylist()[0]["payee_raw"] == "Tsomet Sfarim"


def test_project_top_level_transactions_drops_splits_column() -> None:
    projected = project_top_level_transactions(_sample_transaction_table())

    assert "splits" not in projected.columns
    assert projected.to_dicts()[0]["category_raw"] == "Split"


def test_explode_split_lines_returns_flat_split_rows() -> None:
    exploded = explode_split_lines(_sample_transaction_table())

    assert exploded.height == 2
    assert exploded["parent_transaction_id"].to_list() == ["txn-1", "txn-1"]
    assert exploded["category_id"].to_list() == ["cat-books", "cat-gifts"]
    assert exploded["category_raw"].to_list() == ["Books", "Gifts"]


def test_flat_projection_to_canonical_table_uses_existing_row_ids() -> None:
    flat_df = pl.DataFrame(
        {
            "source": ["bank"],
            "account_name": ["Family Leumi"],
            "source_account": ["Family Leumi"],
            "date": ["2026-03-01"],
            "secondary_date": ["2026-03-02"],
            "txn_kind": ["expense"],
            "merchant_raw": ["Mega Pet"],
            "description_clean": ["Mega Pet"],
            "description_raw": ["Mega Pet Pet Food"],
            "description_clean_norm": ["mega pet"],
            "fingerprint": ["mega pet"],
            "ref": ["123"],
            "outflow_ils": [90.0],
            "inflow_ils": [0.0],
            "bank_txn_id": ["BANK:1"],
            "currency": ["ILS"],
            "amount_bucket": [""],
        }
    )

    canonical = flat_projection_to_canonical_table(
        flat_df,
        artifact_kind="normalized_source_transaction",
        source_system="bank",
    )

    assert canonical["transaction_id"].to_pylist() == ["BANK:1"]
    assert canonical["merchant_raw"].to_pylist() == ["Mega Pet"]
    assert canonical["signed_amount_ils"].to_pylist() == [-90.0]


def test_write_flat_transaction_artifacts_writes_csv_and_parquet(tmp_path) -> None:
    out_path = tmp_path / "normalized.csv"
    flat_df = pl.DataFrame(
        {
            "source": ["card"],
            "account_name": ["Visa"],
            "source_account": ["Visa"],
            "date": ["2026-03-01"],
            "txn_kind": ["expense"],
            "merchant_raw": ["Spotify"],
            "description_clean": ["Spotify"],
            "description_raw": ["Spotify Stockholm"],
            "description_clean_norm": ["spotify"],
            "fingerprint": ["spotify"],
            "outflow_ils": [19.9],
            "inflow_ils": [0.0],
            "card_txn_id": ["CARD:1"],
        }
    )

    csv_path, parquet_path = write_flat_transaction_artifacts(
        flat_df,
        out_path,
        artifact_kind="normalized_source_transaction",
        source_system="card",
    )

    assert csv_path == out_path
    assert parquet_path == out_path.with_suffix(".parquet")
    assert csv_path.exists()
    assert parquet_path.exists()


def test_load_flat_transaction_projection_prefers_sidecar_parquet(tmp_path) -> None:
    out_path = tmp_path / "normalized.csv"
    flat_df = pl.DataFrame(
        {
            "source": ["card"],
            "account_name": ["Visa"],
            "source_account": ["Visa"],
            "date": ["2026-03-01"],
            "txn_kind": ["expense"],
            "merchant_raw": ["Spotify"],
            "description_clean": ["Spotify"],
            "description_raw": ["Spotify Stockholm"],
            "description_clean_norm": ["spotify"],
            "fingerprint": ["spotify-parquet"],
            "outflow_ils": [19.9],
            "inflow_ils": [0.0],
            "card_txn_id": ["CARD:1"],
        }
    )

    write_flat_transaction_artifacts(
        flat_df,
        out_path,
        artifact_kind="normalized_source_transaction",
        source_system="card",
    )
    out_path.write_text(
        "fingerprint,outflow_ils,inflow_ils\nspotify-csv,19.9,0.0\n",
        encoding="utf-8",
    )

    loaded = load_flat_transaction_projection(out_path, prefer_sidecar_parquet=True)

    assert loaded.loc[0, "fingerprint"] == "spotify-parquet"
    assert loaded.loc[0, "source"] == "card"
