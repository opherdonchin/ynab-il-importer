from __future__ import annotations

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

    raw = pd.read_csv(map_path, dtype="string").fillna("")
    required_cols = {"source_account", "ynab_account_name"}
    if not required_cols.issubset(raw.columns):
        _warn_unmatched(
            source=source,
            message=(
                f"Account map file '{map_path}' missing required columns "
                f"{sorted(required_cols)}"
            ),
            unmatched_accounts=source_accounts,
        )
        return out

    raw["source_account"] = _normalize_text_series(raw["source_account"])
    raw["ynab_account_name"] = _normalize_text_series(raw["ynab_account_name"])
    if "ynab_account_id" in raw.columns:
        raw["ynab_account_id"] = _normalize_text_series(raw["ynab_account_id"])
    raw = raw[(raw["source_account"] != "") & (raw["ynab_account_name"] != "")]

    if "source" in raw.columns:
        source_series = _normalize_text_series(raw["source"]).str.lower()
        source_key = str(source).strip().lower()
        raw = raw[(source_series == "") | (source_series == source_key)]

    mapping = {
        row["source_account"]: row["ynab_account_name"]
        for _, row in raw.iterrows()
    }
    id_mapping = {}
    if "ynab_account_id" in raw.columns:
        id_mapping = {
            row["source_account"]: row["ynab_account_id"]
            for _, row in raw.iterrows()
            if str(row.get("ynab_account_id", "")).strip() != ""
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
