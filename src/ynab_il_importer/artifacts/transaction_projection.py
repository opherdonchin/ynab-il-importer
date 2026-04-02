from __future__ import annotations

from typing import Any

import polars as pl

from ynab_il_importer.artifacts.transaction_io import normalize_transaction_table


def project_top_level_transactions(
    data: Any,
    *,
    drop_splits: bool = True,
) -> pl.DataFrame:
    df = pl.from_arrow(normalize_transaction_table(data))
    if drop_splits and "splits" in df.columns:
        return df.drop("splits")
    return df


def explode_split_lines(data: Any) -> pl.DataFrame:
    df = pl.from_arrow(normalize_transaction_table(data))
    if "splits" not in df.columns:
        return pl.DataFrame(
            schema={
                "parent_transaction_id": pl.String,
                "split_id": pl.String,
                "ynab_subtransaction_id": pl.String,
                "payee_raw": pl.String,
                "category_id": pl.String,
                "category_raw": pl.String,
                "memo": pl.String,
                "inflow_ils": pl.Float64,
                "outflow_ils": pl.Float64,
                "import_id": pl.String,
                "matched_transaction_id": pl.String,
            }
        )

    exploded = df.select("transaction_id", "splits").explode("splits")
    exploded = exploded.filter(pl.col("splits").is_not_null())
    if exploded.is_empty():
        return pl.DataFrame(
            schema={
                "parent_transaction_id": pl.String,
                "split_id": pl.String,
                "ynab_subtransaction_id": pl.String,
                "payee_raw": pl.String,
                "category_id": pl.String,
                "category_raw": pl.String,
                "memo": pl.String,
                "inflow_ils": pl.Float64,
                "outflow_ils": pl.Float64,
                "import_id": pl.String,
                "matched_transaction_id": pl.String,
            }
        )

    return exploded.select(
        pl.col("transaction_id").alias("parent_transaction_id"),
        pl.col("splits").struct.field("split_id").alias("split_id"),
        pl.col("splits").struct.field("ynab_subtransaction_id").alias("ynab_subtransaction_id"),
        pl.col("splits").struct.field("payee_raw").alias("payee_raw"),
        pl.col("splits").struct.field("category_id").alias("category_id"),
        pl.col("splits").struct.field("category_raw").alias("category_raw"),
        pl.col("splits").struct.field("memo").alias("memo"),
        pl.col("splits").struct.field("inflow_ils").alias("inflow_ils"),
        pl.col("splits").struct.field("outflow_ils").alias("outflow_ils"),
        pl.col("splits").struct.field("import_id").alias("import_id"),
        pl.col("splits").struct.field("matched_transaction_id").alias("matched_transaction_id"),
    )
