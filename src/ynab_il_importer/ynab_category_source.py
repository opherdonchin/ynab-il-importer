from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa

from ynab_il_importer.artifacts.transaction_io import (
    normalize_transaction_table,
    read_transactions_arrow,
)
from ynab_il_importer.artifacts.transaction_schema import empty_transaction_table
import ynab_il_importer.fingerprint as fingerprint_mod
import ynab_il_importer.ynab_api as ynab_api


def build_category_source_canonical(
    source_path: Path,
    *,
    category_name: str = "",
    category_id: str = "",
    target_account_name: str,
    target_account_id: str = "",
    use_fingerprint_map: bool,
    fingerprint_map_path: Path,
    fingerprint_log_path: Path,
) -> pa.Table:
    wanted_category_name = str(category_name or "").strip()
    wanted_category_id = str(category_id or "").strip()
    target_account = str(target_account_name or "").strip()
    if not wanted_category_name and not wanted_category_id:
        raise ValueError(
            "build_category_source_canonical() requires category_name or category_id."
        )
    if not target_account:
        raise ValueError(
            "build_category_source_canonical() requires target_account_name."
        )

    source_df = ynab_api.project_category_transactions_to_source_rows(
        read_transactions_arrow(source_path),
        category_id=wanted_category_id or None,
        category_name=wanted_category_name or None,
    )
    if source_df.empty:
        return empty_transaction_table()

    original_account = source_df["account_name"].astype("string").fillna("").str.strip()
    payee_series = source_df["payee_raw"].astype("string").fillna("").str.strip()
    memo_series = source_df["memo"].astype("string").fillna("").str.strip()
    category_series = source_df["category_raw"].astype("string").fillna("").str.strip()

    source_df["source"] = "ynab"
    source_df["source_account"] = original_account
    source_df["account_name"] = target_account
    source_df["account_id"] = str(target_account_id or "").strip()
    source_df["secondary_date"] = ""
    source_df["merchant_raw"] = payee_series
    source_df["description_raw"] = memo_series.where(memo_series != "", payee_series)
    source_df["description_clean"] = payee_series.where(
        payee_series != "",
        source_df["description_raw"].astype("string").fillna("").str.strip(),
    )
    source_df["ref"] = (
        source_df["parent_ynab_id"].astype("string").fillna("").str.strip()
    )

    source_df = fingerprint_mod.apply_fingerprints(
        source_df,
        use_fingerprint_map=use_fingerprint_map,
        fingerprint_map_path=fingerprint_map_path,
        log_path=fingerprint_log_path,
    )

    # Keep the original category context from Family while aligning the account
    # to the Aikido target account for strict exact date/amount/account pairing.
    source_df["category_raw"] = category_series
    source_df["transaction_id"] = source_df["ynab_id"].astype("string").fillna("").str.strip()
    source_df["parent_transaction_id"] = source_df["transaction_id"]
    source_df["artifact_kind"] = "normalized_source_transaction"
    source_df["artifact_version"] = "transaction_v1"
    source_df["source_system"] = "ynab_category"
    source_df["date"] = (
        pd.to_datetime(source_df["date"], errors="coerce")
        .dt.strftime("%Y-%m-%d")
        .fillna("")
    )
    source_df["signed_amount_ils"] = source_df["inflow_ils"] - source_df["outflow_ils"]
    source_df["balance_ils"] = 0.0

    canonical_df = pl.from_pandas(source_df)
    return normalize_transaction_table(canonical_df, allow_extra_columns=True)
