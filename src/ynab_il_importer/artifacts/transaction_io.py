from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ynab_il_importer.artifacts.transaction_schema import TRANSACTION_SCHEMA


def _to_arrow_table(data: Any) -> pa.Table:
    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pl.DataFrame):
        return data.to_arrow()
    if isinstance(data, pd.DataFrame):
        return pa.Table.from_pandas(data, preserve_index=False)
    raise TypeError("transactions must be a pandas DataFrame, polars DataFrame, or pyarrow Table")


def normalize_transaction_table(
    data: Any,
    *,
    schema: pa.Schema = TRANSACTION_SCHEMA,
    allow_extra_columns: bool = False,
) -> pa.Table:
    table = _to_arrow_table(data)
    extra_columns = [name for name in table.column_names if name not in schema.names]
    if extra_columns and not allow_extra_columns:
        raise ValueError(f"Unexpected transaction columns: {extra_columns}")

    arrays: list[pa.Array | pa.ChunkedArray] = []
    for field in schema:
        if field.name in table.column_names:
            arrays.append(table[field.name].cast(field.type, safe=False))
        else:
            arrays.append(pa.nulls(table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def write_transactions_parquet(
    data: Any,
    path: str | Path,
    *,
    schema: pa.Schema = TRANSACTION_SCHEMA,
    allow_extra_columns: bool = False,
) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = normalize_transaction_table(
        data,
        schema=schema,
        allow_extra_columns=allow_extra_columns,
    )
    pq.write_table(table, output_path)


def read_transactions_arrow(
    path: str | Path,
    *,
    schema: pa.Schema = TRANSACTION_SCHEMA,
) -> pa.Table:
    table = pq.read_table(Path(path))
    return normalize_transaction_table(table, schema=schema, allow_extra_columns=True)


def read_transactions_polars(
    path: str | Path,
    *,
    schema: pa.Schema = TRANSACTION_SCHEMA,
) -> pl.DataFrame:
    return pl.from_arrow(read_transactions_arrow(path, schema=schema))


def read_transactions_pandas(
    path: str | Path,
    *,
    schema: pa.Schema = TRANSACTION_SCHEMA,
) -> pd.DataFrame:
    return read_transactions_arrow(path, schema=schema).to_pandas()
