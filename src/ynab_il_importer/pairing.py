from __future__ import annotations

from typing import Any

import polars as pl

import ynab_il_importer.normalize as normalize


BANK_RAW_CANDIDATES = ["description_clean", "merchant_raw", "description_raw"]
CARD_RAW_CANDIDATES = ["description_clean", "description_raw", "merchant_raw"]
DEFAULT_RAW_CANDIDATES = ["description_clean", "merchant_raw", "description_raw", "raw_text"]
JOIN_KEY_COLUMNS = ["account_key", "date_key", "amount_key"]
PAIR_COLUMNS = [
    "source_type",
    "source_file",
    "source_account",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "raw_text",
    "raw_norm",
    "fingerprint",
    "ynab_file",
    "ynab_account_id",
    "ynab_account",
    "ynab_outflow_ils",
    "ynab_inflow_ils",
    "ynab_payee_raw",
    "ynab_category_raw",
    "ynab_fingerprint",
    "ynab_id",
    "ynab_import_id",
    "ynab_matched_transaction_id",
    "ynab_cleared",
    "ynab_approved",
    "ambiguous_key",
]
SOURCE_PREPARED_SCHEMA = {
    "source_type": pl.Utf8,
    "source_file": pl.Utf8,
    "source_account": pl.Utf8,
    "account_name": pl.Utf8,
    "date": pl.Date,
    "outflow_ils": pl.Float64,
    "inflow_ils": pl.Float64,
    "raw_text": pl.Utf8,
    "fingerprint": pl.Utf8,
    "account_key": pl.Utf8,
    "date_key": pl.Date,
    "amount_key": pl.Float64,
}
YNAB_PREPARED_SCHEMA = {
    "account_key": pl.Utf8,
    "date_key": pl.Date,
    "amount_key": pl.Float64,
    "ynab_file": pl.Utf8,
    "ynab_account_id": pl.Utf8,
    "ynab_account": pl.Utf8,
    "ynab_outflow_ils": pl.Float64,
    "ynab_inflow_ils": pl.Float64,
    "ynab_payee_raw": pl.Utf8,
    "ynab_category_raw": pl.Utf8,
    "ynab_fingerprint": pl.Utf8,
    "ynab_id": pl.Utf8,
    "ynab_import_id": pl.Utf8,
    "ynab_matched_transaction_id": pl.Utf8,
    "ynab_cleared": pl.Utf8,
    "ynab_approved": pl.Utf8,
}
PAIR_SCHEMA = {
    "source_type": pl.Utf8,
    "source_file": pl.Utf8,
    "source_account": pl.Utf8,
    "account_name": pl.Utf8,
    "date": pl.Date,
    "outflow_ils": pl.Float64,
    "inflow_ils": pl.Float64,
    "raw_text": pl.Utf8,
    "fingerprint": pl.Utf8,
    "ynab_file": pl.Utf8,
    "ynab_account_id": pl.Utf8,
    "ynab_account": pl.Utf8,
    "ynab_outflow_ils": pl.Float64,
    "ynab_inflow_ils": pl.Float64,
    "ynab_payee_raw": pl.Utf8,
    "ynab_category_raw": pl.Utf8,
    "ynab_fingerprint": pl.Utf8,
    "ynab_id": pl.Utf8,
    "ynab_import_id": pl.Utf8,
    "ynab_matched_transaction_id": pl.Utf8,
    "ynab_cleared": pl.Utf8,
    "ynab_approved": pl.Utf8,
    "account_key": pl.Utf8,
    "date_key": pl.Date,
    "amount_key": pl.Float64,
}
EMPTY_PAIR_SCHEMA = {**{col: dtype for col, dtype in PAIR_SCHEMA.items() if col in PAIR_COLUMNS}, "raw_norm": pl.Utf8, "ambiguous_key": pl.Boolean}


def _empty_frame(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def _date_series(df: pl.DataFrame, col: str) -> pl.Series:
    if col not in df.columns:
        return pl.Series(col, [None] * len(df), dtype=pl.Date)
    series = df[col]
    if series.dtype == pl.Date:
        return series
    return (
        series.cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_date(strict=False)
    )


def _series_or_default(
    df: pl.DataFrame, col: str, dtype: pl.PolarsDataType, default: Any = None
) -> pl.Series:
    if col in df.columns:
        return df[col].cast(dtype, strict=False)
    return pl.Series(col, [default] * len(df), dtype=dtype)


def _pick_raw_text(df: pl.DataFrame, columns: list[str]) -> pl.Series:
    for col in columns:
        if col in df.columns:
            series = df[col].cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
            if bool(series.ne("").any()):
                return series
    return pl.Series("raw_text", [""] * len(df), dtype=pl.Utf8)


def _pick_raw_text_by_source(
    df: pl.DataFrame, source_name: str, columns: list[str]
) -> pl.Series:
    if "source" not in df.columns:
        return pl.Series("raw_text", [""] * len(df), dtype=pl.Utf8)
    source_values = (
        df["source"]
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_lowercase()
    )
    mask_values = [value == source_name for value in source_values.to_list()]
    subset = df.filter(pl.Series(mask_values, dtype=pl.Boolean))
    picked = _pick_raw_text(subset, columns).to_list()
    result = [""] * len(df)
    picked_index = 0
    for idx, matches in enumerate(mask_values):
        if not matches:
            continue
        result[idx] = picked[picked_index]
        picked_index += 1
    return pl.Series("raw_text", result, dtype=pl.Utf8)


def _prepare_source(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return _empty_frame(SOURCE_PREPARED_SCHEMA)

    source_type = (
        _series_or_default(df, "source", pl.Utf8, "")
        .fill_null("")
        .str.strip_chars()
        .str.to_lowercase()
    )
    source_type_values = [value if value else "source" for value in source_type.to_list()]
    source_type = pl.Series("source", source_type_values, dtype=pl.Utf8)
    account_name = (
        _series_or_default(df, "account_name", pl.Utf8, "")
        .fill_null("")
        .str.strip_chars()
    )
    source_account = (
        _series_or_default(df, "source_account", pl.Utf8, "")
        .fill_null("")
        .str.strip_chars()
    )
    outflow_ils = (
        _series_or_default(df, "outflow_ils", pl.Float64, 0.0).fill_null(0.0).round(2)
    )
    inflow_ils = (
        _series_or_default(df, "inflow_ils", pl.Float64, 0.0).fill_null(0.0).round(2)
    )
    if "outflow_ils" not in df.columns and "inflow_ils" not in df.columns:
        amount = (
            _series_or_default(df, "amount_ils", pl.Float64, 0.0).fill_null(0.0).round(2)
        )
        amount_values = amount.to_list()
        outflow_ils = pl.Series(
            "outflow_ils",
            [round(abs(value), 2) if value < 0 else 0.0 for value in amount_values],
            dtype=pl.Float64,
        )
        inflow_ils = pl.Series(
            "inflow_ils",
            [round(value, 2) if value > 0 else 0.0 for value in amount_values],
            dtype=pl.Float64,
        )

    source_df = df.with_columns(source_type)
    bank_raw = _pick_raw_text_by_source(source_df, "bank", BANK_RAW_CANDIDATES).to_list()
    card_raw = _pick_raw_text_by_source(source_df, "card", CARD_RAW_CANDIDATES).to_list()
    default_raw = _pick_raw_text(source_df, DEFAULT_RAW_CANDIDATES).to_list()
    raw_text = pl.Series(
        "raw_text",
        [
            bank if bank else card if card else default
            for bank, card, default in zip(bank_raw, card_raw, default_raw, strict=False)
        ],
        dtype=pl.Utf8,
    )
    fingerprint = (
        _series_or_default(df, "fingerprint", pl.Utf8, "")
        .fill_null("")
        .str.strip_chars()
    )
    if bool(fingerprint.eq("").any()):
        raise ValueError("Source data missing fingerprint values; run fingerprinting first.")

    prepared = pl.DataFrame(
        {
            "source_type": pl.Series("source_type", source_type_values, dtype=pl.Utf8),
            "source_file": (
                _series_or_default(df, "source_file", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
            "source_account": source_account,
            "account_name": account_name,
            "date": _date_series(df, "date"),
            "outflow_ils": outflow_ils,
            "inflow_ils": inflow_ils,
            "raw_text": raw_text,
            "fingerprint": fingerprint,
        }
    ).with_columns(
        pl.col("account_name").alias("account_key"),
        pl.col("date").alias("date_key"),
        (pl.col("inflow_ils") - pl.col("outflow_ils")).round(2).alias("amount_key"),
    )
    return prepared.drop_nulls(subset=["account_key", "date_key", "amount_key"])


def _prepare_ynab(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return _empty_frame(YNAB_PREPARED_SCHEMA)

    ynab_account = (
        _series_or_default(df, "account_name", pl.Utf8, "")
        .fill_null("")
        .str.strip_chars()
    )
    outflow_ils = (
        _series_or_default(df, "outflow_ils", pl.Float64, 0.0).fill_null(0.0).round(2)
    )
    inflow_ils = (
        _series_or_default(df, "inflow_ils", pl.Float64, 0.0).fill_null(0.0).round(2)
    )
    if "outflow_ils" not in df.columns and "inflow_ils" not in df.columns:
        amount = (
            _series_or_default(df, "amount_ils", pl.Float64, 0.0).fill_null(0.0).round(2)
        )
        amount_values = amount.to_list()
        outflow_ils = pl.Series(
            "ynab_outflow_ils",
            [round(abs(value), 2) if value < 0 else 0.0 for value in amount_values],
            dtype=pl.Float64,
        )
        inflow_ils = pl.Series(
            "ynab_inflow_ils",
            [round(value, 2) if value > 0 else 0.0 for value in amount_values],
            dtype=pl.Float64,
        )
    else:
        outflow_ils = outflow_ils.rename("ynab_outflow_ils")
        inflow_ils = inflow_ils.rename("ynab_inflow_ils")

    prepared = pl.DataFrame(
        {
            "ynab_account_id": (
                _series_or_default(df, "account_id", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
            "date_key": _date_series(df, "date"),
            "amount_key": (inflow_ils - outflow_ils).round(2),
            "ynab_file": (
                _series_or_default(df, "ynab_file", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
            "ynab_account": ynab_account,
            "ynab_outflow_ils": outflow_ils,
            "ynab_inflow_ils": inflow_ils,
            "ynab_payee_raw": _series_or_default(df, "payee_raw", pl.Utf8, "").fill_null(""),
            "ynab_category_raw": _series_or_default(df, "category_raw", pl.Utf8, "").fill_null(""),
            "ynab_fingerprint": (
                _series_or_default(df, "fingerprint", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
            "ynab_id": (
                _series_or_default(df, "ynab_id", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
            "ynab_import_id": (
                _series_or_default(df, "import_id", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
            "ynab_matched_transaction_id": (
                _series_or_default(df, "matched_transaction_id", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
            "ynab_cleared": (
                _series_or_default(df, "cleared", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
            "ynab_approved": (
                _series_or_default(df, "approved", pl.Utf8, "")
                .fill_null("")
                .str.strip_chars()
            ),
        }
    ).with_columns(pl.col("ynab_account").alias("account_key"))
    return prepared.drop_nulls(subset=["account_key", "date_key", "amount_key"])


def _join_pairs(source_df: pl.DataFrame, ynab_df: pl.DataFrame) -> pl.DataFrame:
    if source_df.is_empty() or ynab_df.is_empty():
        return _empty_frame(PAIR_SCHEMA)

    joined = source_df.join(ynab_df, on=JOIN_KEY_COLUMNS, how="inner")
    return joined.select(list(PAIR_SCHEMA))


def match_pairs(source_df: pl.DataFrame, ynab_df: pl.DataFrame) -> pl.DataFrame:
    pairs = _join_pairs(_prepare_source(source_df), _prepare_ynab(ynab_df))
    if pairs.is_empty():
        return _empty_frame(EMPTY_PAIR_SCHEMA)

    pairs = pairs.with_columns(
        pl.col("raw_text")
        .map_elements(normalize.normalize_text, return_dtype=pl.Utf8)
        .alias("raw_norm")
    )
    key_counts = pairs.group_by(JOIN_KEY_COLUMNS).agg(pl.len().alias("_key_count"))
    pairs = (
        pairs.join(key_counts, on=JOIN_KEY_COLUMNS, how="left")
        .with_columns((pl.col("_key_count").fill_null(0) > 1).alias("ambiguous_key"))
        .select(PAIR_COLUMNS)
    )
    return pairs
