from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable
import warnings

import pandas as pd


DEFAULT_ACCOUNT_MAP_PATH = Path("mappings/account_name_map.csv")


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _normalized_unique(values: Iterable[str]) -> list[str]:
    unique = sorted({str(v).strip() for v in values if str(v).strip() != ""}, key=str.casefold)
    return unique


def _warn_unmatched(source: str, message: str, unmatched_accounts: list[str]) -> None:
    if not unmatched_accounts:
        return
    joined = ", ".join(unmatched_accounts)
    warnings.warn(
        f"{message} for source='{source}'. Unmatched account names: {joined}",
        UserWarning,
        stacklevel=2,
    )


def apply_account_name_map(
    df: pd.DataFrame,
    source: str,
    account_map_path: str | Path = DEFAULT_ACCOUNT_MAP_PATH,
) -> pd.DataFrame:
    out = df.copy()
    if "account_name" not in out.columns:
        out["account_name"] = ""
    out["account_name"] = _normalize_text_series(out["account_name"])

    source_col = "source_account" if "source_account" in out.columns else "account_name"
    out[source_col] = _normalize_text_series(out[source_col])

    source_accounts = _normalized_unique(out[source_col].tolist())
    if not source_accounts:
        return out

    map_path = Path(account_map_path)
    if not map_path.exists():
        _warn_unmatched(
            source=source,
            message=f"Account map file not found at '{map_path}'",
            unmatched_accounts=source_accounts,
        )
        return out

    with open(map_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        raw_rows = list(reader)
    raw_columns = set(reader.fieldnames or [])
    required_cols = {"source_account", "ynab_account_name"}
    if not required_cols.issubset(raw_columns):
        _warn_unmatched(
            source=source,
            message=(
                f"Account map file '{map_path}' missing required columns "
                f"{sorted(required_cols)}"
            ),
            unmatched_accounts=source_accounts,
        )
        return out

    source_key = str(source).strip().lower()
    filtered_rows = []
    for row in raw_rows:
        src_acc = row.get("source_account", "").strip()
        ynab_acc = row.get("ynab_account_name", "").strip()
        if not src_acc or not ynab_acc:
            continue
        row_source = row.get("source", "").strip().lower()
        if row_source and row_source != source_key:
            continue
        filtered_rows.append(
            {
                "source_account": src_acc,
                "ynab_account_name": ynab_acc,
                "ynab_account_id": row.get("ynab_account_id", "").strip(),
            }
        )

    mapping = {r["source_account"]: r["ynab_account_name"] for r in filtered_rows}
    id_mapping = {
        r["source_account"]: r["ynab_account_id"]
        for r in filtered_rows
        if r["ynab_account_id"]
    }
    if not mapping:
        _warn_unmatched(
            source=source,
            message=f"Account map file '{map_path}' has no usable rows",
            unmatched_accounts=source_accounts,
        )
        return out

    original_accounts = out[source_col].copy()
    mapped = original_accounts.map(mapping).astype("string")
    has_map = mapped.notna() & (mapped.str.strip() != "")
    out.loc[has_map, "account_name"] = mapped.loc[has_map].str.strip()

    if id_mapping:
        if "ynab_account_id" not in out.columns:
            out["ynab_account_id"] = ""
        mapped_ids = original_accounts.map(id_mapping).astype("string")
        has_id = mapped_ids.notna() & (mapped_ids.str.strip() != "")
        out.loc[has_id, "ynab_account_id"] = mapped_ids.loc[has_id].str.strip()

    unmatched_accounts = _normalized_unique(out.loc[~has_map, source_col].tolist())
    _warn_unmatched(
        source=source,
        message=f"Account map file '{map_path}' does not include all accounts",
        unmatched_accounts=unmatched_accounts,
    )
    return out
