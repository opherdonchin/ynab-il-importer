from __future__ import annotations

import polars as pl
import pyarrow as pa

from ynab_il_importer.artifacts.transaction_io import (
    normalize_transaction_table,
    read_transactions_arrow,
    read_transactions_pandas,
    read_transactions_polars,
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
    loaded_pandas = read_transactions_pandas(path)

    assert loaded_arrow["splits"].to_pylist()[0][0]["category_raw"] == "Books"
    assert isinstance(loaded_polars, pl.DataFrame)
    assert loaded_polars.height == 1
    assert loaded_pandas.loc[0, "payee_raw"] == "Tsomet Sfarim"


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
