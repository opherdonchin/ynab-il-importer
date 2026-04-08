from __future__ import annotations
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ynab_il_importer.artifacts.transaction_schema import TRANSACTION_SCHEMA


def _to_arrow_table(data: Any) -> pa.Table:
    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pl.DataFrame):
        return data.to_arrow()
    if type(data).__module__.startswith("pandas"):
        return pa.Table.from_pandas(data, preserve_index=False)
    raise TypeError("transactions must be a polars DataFrame or pyarrow Table")


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
    df = pl.from_arrow(_to_arrow_table(data))
    row_count = df.height

    def _text_expr(name: str, default: str = "") -> pl.Expr:
        if name in df.columns:
            return pl.col(name).cast(pl.String, strict=False).fill_null("").str.strip_chars()
        return pl.lit(default, dtype=pl.String)

    def _float_expr(name: str) -> pl.Expr:
        if name in df.columns:
            return pl.col(name).cast(pl.Float64, strict=False).fill_null(0.0)
        return pl.lit(0.0, dtype=pl.Float64)

    def _bool_expr(name: str, default: bool = False) -> pl.Expr:
        if name in df.columns:
            return (
                pl.when(pl.col(name).cast(pl.Boolean, strict=False).is_not_null())
                .then(pl.col(name).cast(pl.Boolean, strict=False))
                .otherwise(
                    pl.col(name)
                    .cast(pl.String, strict=False)
                    .fill_null("")
                    .str.strip_chars()
                    .str.to_lowercase()
                    .is_in(["true", "1", "yes", "y"])
                )
                .fill_null(default)
            )
        return pl.lit(default, dtype=pl.Boolean)

    def _date_expr(name: str) -> pl.Expr:
        if name not in df.columns:
            return pl.lit("", dtype=pl.String)
        return (
            pl.col(name)
            .cast(pl.String, strict=False)
            .fill_null("")
            .str.strip_chars()
            .str.to_datetime(strict=False)
            .dt.strftime("%Y-%m-%d")
            .fill_null("")
        )

    transaction_id = _text_expr("transaction_id")
    for fallback in ("bank_txn_id", "card_txn_id", "ynab_id", "import_id"):
        fallback_values = _text_expr(fallback)
        transaction_id = pl.when(transaction_id == "").then(fallback_values).otherwise(transaction_id)
    if row_count:
        generated = pl.Series(
            "generated_transaction_id",
            [f"{source_system}:{idx}" for idx in range(row_count)],
            dtype=pl.String,
        )
        transaction_id = pl.when(transaction_id == "").then(generated).otherwise(transaction_id)

    account_id = _text_expr("account_id")
    account_id = pl.when(account_id == "").then(_text_expr("ynab_account_id")).otherwise(account_id)

    inflow = _float_expr("inflow_ils")
    outflow = _float_expr("outflow_ils")
    canonical_df = df.select(
        pl.lit(artifact_kind, dtype=pl.String).alias("artifact_kind"),
        pl.lit("transaction_v1", dtype=pl.String).alias("artifact_version"),
        pl.lit(source_system, dtype=pl.String).alias("source_system"),
        transaction_id.alias("transaction_id"),
        _text_expr("ynab_id").alias("ynab_id"),
        _text_expr("import_id").alias("import_id"),
        transaction_id.alias("parent_transaction_id"),
        account_id.alias("account_id"),
        _text_expr("account_name").alias("account_name"),
        _text_expr("source_account").alias("source_account"),
        _date_expr("date").alias("date"),
        _date_expr("secondary_date").alias("secondary_date"),
        inflow.alias("inflow_ils"),
        outflow.alias("outflow_ils"),
        (inflow - outflow).alias("signed_amount_ils"),
        _float_expr("balance_ils").alias("balance_ils"),
        pl.when(_text_expr("payee_raw") == "")
        .then(_text_expr("merchant_raw"))
        .otherwise(_text_expr("payee_raw"))
        .alias("payee_raw"),
        _text_expr("category_id").alias("category_id"),
        _text_expr("category_raw").alias("category_raw"),
        _text_expr("memo").alias("memo"),
        _text_expr("txn_kind").alias("txn_kind"),
        _text_expr("fingerprint").alias("fingerprint"),
        _text_expr("description_raw").alias("description_raw"),
        _text_expr("description_clean").alias("description_clean"),
        _text_expr("description_clean_norm").alias("description_clean_norm"),
        _text_expr("merchant_raw").alias("merchant_raw"),
        _text_expr("ref").alias("ref"),
        _text_expr("matched_transaction_id").alias("matched_transaction_id"),
        _text_expr("cleared").alias("cleared"),
        _bool_expr("approved").alias("approved"),
        _bool_expr("is_subtransaction").alias("is_subtransaction"),
        pl.lit(None, dtype=pl.Null).alias("splits"),
    )
    return normalize_transaction_table(canonical_df)


def write_flat_transaction_artifacts(
    data: pl.DataFrame,
    csv_path: str | Path,
    *,
    artifact_kind: str,
    source_system: str,
) -> tuple[Path, Path]:
    output_path = Path(csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    canonical = flat_projection_to_canonical_table(
        data,
        artifact_kind=artifact_kind,
        source_system=source_system,
    )
    parquet_path = output_path.with_suffix(".parquet")
    write_transactions_parquet(canonical, parquet_path)
    data.write_csv(output_path, include_bom=True)
    return output_path, parquet_path


def write_canonical_transaction_artifacts(
    data: Any,
    path: str | Path,
    *,
    csv_projection: Any | None = None,
    schema: pa.Schema = TRANSACTION_SCHEMA,
    allow_extra_columns: bool = False,
) -> tuple[Path | None, Path]:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = normalize_transaction_table(
        data,
        schema=schema,
        allow_extra_columns=allow_extra_columns,
    )

    if output_path.suffix.lower() == ".parquet":
        write_transactions_parquet(table, output_path, schema=schema, allow_extra_columns=True)
        return None, output_path

    parquet_path = output_path.with_suffix(".parquet")
    write_transactions_parquet(table, parquet_path, schema=schema, allow_extra_columns=True)
    if csv_projection is None:
        from ynab_il_importer.artifacts.transaction_projection import (
            project_top_level_transactions,
        )

        flat_df = project_top_level_transactions(table)
    elif isinstance(csv_projection, pl.DataFrame):
        flat_df = csv_projection
    elif isinstance(csv_projection, pa.Table):
        flat_df = pl.from_arrow(csv_projection)
    else:
        raise TypeError(
            "csv_projection must be a polars DataFrame, pyarrow Table, or None"
        )
    flat_df.write_csv(output_path, include_bom=True)
    return output_path, parquet_path


def load_flat_transaction_projection(
    path: str | Path,
    *,
    prefer_sidecar_parquet: bool = True,
) -> pl.DataFrame:
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

        projected = project_top_level_transactions(read_transactions_arrow(resolved_path))
        if "source" not in projected.columns and "source_system" in projected.columns:
            projected = projected.with_columns(pl.col("source_system").alias("source"))
        return projected

    return pl.read_csv(source_path)


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
