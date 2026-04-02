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


def flat_projection_to_canonical_table(
    data: Any,
    *,
    artifact_kind: str,
    source_system: str,
) -> pa.Table:
    df = _to_arrow_table(data).to_pandas().copy()
    row_count = len(df)

    def _text_column(name: str, default: str = "") -> pd.Series:
        if name in df.columns:
            return df[name].astype("string").fillna("").str.strip()
        return pd.Series([default] * row_count, index=df.index, dtype="string")

    def _float_column(name: str) -> pd.Series:
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce").fillna(0.0).astype(float)
        return pd.Series([0.0] * row_count, index=df.index, dtype="float64")

    def _bool_column(name: str, default: bool = False) -> pd.Series:
        if name in df.columns:
            values = df[name]
            if pd.api.types.is_bool_dtype(values):
                return values.fillna(default).astype(bool)
            normalized = values.astype("string").fillna("").str.strip().str.casefold()
            truthy = {"true", "1", "yes", "y"}
            return normalized.isin(truthy)
        return pd.Series([default] * row_count, index=df.index, dtype="bool")

    def _date_column(name: str) -> pd.Series:
        if name not in df.columns:
            return pd.Series([""] * row_count, index=df.index, dtype="string")
        parsed = pd.to_datetime(df[name], errors="coerce")
        return parsed.dt.strftime("%Y-%m-%d").fillna("").astype("string")

    transaction_id = _text_column("transaction_id")
    for fallback in ("bank_txn_id", "card_txn_id", "ynab_id", "import_id"):
        fallback_values = _text_column(fallback)
        transaction_id = transaction_id.mask(transaction_id == "", fallback_values)
    if (transaction_id == "").any():
        generated = pd.Series(
            [f"{source_system}:{idx}" for idx in range(row_count)],
            index=df.index,
            dtype="string",
        )
        transaction_id = transaction_id.mask(transaction_id == "", generated)

    inflow = _float_column("inflow_ils")
    outflow = _float_column("outflow_ils")
    canonical_df = pd.DataFrame(
        {
            "artifact_kind": artifact_kind,
            "artifact_version": "transaction_v1",
            "source_system": source_system,
            "transaction_id": transaction_id,
            "ynab_id": _text_column("ynab_id"),
            "import_id": _text_column("import_id"),
            "parent_transaction_id": transaction_id,
            "account_id": _text_column("account_id"),
            "account_name": _text_column("account_name"),
            "source_account": _text_column("source_account"),
            "date": _date_column("date"),
            "secondary_date": _date_column("secondary_date"),
            "inflow_ils": inflow,
            "outflow_ils": outflow,
            "signed_amount_ils": (inflow - outflow).astype(float),
            "payee_raw": _text_column("payee_raw").mask(
                _text_column("payee_raw") == "",
                _text_column("merchant_raw"),
            ),
            "category_id": _text_column("category_id"),
            "category_raw": _text_column("category_raw"),
            "memo": _text_column("memo"),
            "txn_kind": _text_column("txn_kind"),
            "fingerprint": _text_column("fingerprint"),
            "description_raw": _text_column("description_raw"),
            "description_clean": _text_column("description_clean"),
            "description_clean_norm": _text_column("description_clean_norm"),
            "merchant_raw": _text_column("merchant_raw"),
            "ref": _text_column("ref"),
            "matched_transaction_id": _text_column("matched_transaction_id"),
            "cleared": _text_column("cleared"),
            "approved": _bool_column("approved"),
            "is_subtransaction": _bool_column("is_subtransaction"),
            "splits": pd.Series([None] * row_count, index=df.index, dtype="object"),
        }
    )
    return normalize_transaction_table(canonical_df)


def write_flat_transaction_artifacts(
    data: Any,
    csv_path: str | Path,
    *,
    artifact_kind: str,
    source_system: str,
) -> tuple[Path, Path]:
    output_path = Path(csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        flat_df = data.copy()
    elif isinstance(data, pl.DataFrame):
        flat_df = data.to_pandas()
    elif isinstance(data, pa.Table):
        flat_df = data.to_pandas()
    else:
        raise TypeError(
            "flat transaction artifacts must be written from a pandas DataFrame, polars DataFrame, or pyarrow Table"
        )
    canonical = flat_projection_to_canonical_table(
        flat_df,
        artifact_kind=artifact_kind,
        source_system=source_system,
    )
    parquet_path = output_path.with_suffix(".parquet")
    write_transactions_parquet(canonical, parquet_path)
    flat_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path, parquet_path


def load_flat_transaction_projection(
    path: str | Path,
    *,
    prefer_sidecar_parquet: bool = True,
) -> pd.DataFrame:
    source_path = Path(path)
    resolved_path = source_path
    if source_path.suffix.lower() == ".csv" and prefer_sidecar_parquet:
        sidecar = source_path.with_suffix(".parquet")
        if sidecar.exists():
            resolved_path = sidecar

    if resolved_path.suffix.lower() == ".parquet":
        from ynab_il_importer.artifacts.transaction_projection import (
            project_top_level_transactions,
        )

        return project_top_level_transactions(read_transactions_arrow(resolved_path)).to_pandas()

    return pd.read_csv(source_path, dtype="string").fillna("")


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
