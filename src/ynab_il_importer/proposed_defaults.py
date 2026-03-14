from __future__ import annotations

from typing import Any

import pandas as pd


UNCATEGORIZED_CATEGORY = "Uncategorized"
_TRANSFER_PREFIX = "Transfer :"


def _string_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column].astype("string").fillna("").str.strip()
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _is_transfer_payee(value: Any) -> bool:
    return str(value or "").strip().startswith(_TRANSFER_PREFIX)


def apply_default_selections(
    df: pd.DataFrame,
    *,
    uncategorized_category: str = UNCATEGORIZED_CATEGORY,
    only_unreviewed: bool = True,
) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out

    if only_unreviewed and "reviewed" in out.columns:
        eligible = ~out["reviewed"].astype(bool).fillna(False)
    else:
        eligible = pd.Series([True] * len(out), index=out.index)

    payee_selected = _string_series(out, "payee_selected")
    payee_options = _string_series(out, "payee_options")
    fingerprint = _string_series(out, "fingerprint")
    payee_fallback = eligible & payee_selected.eq("") & payee_options.eq("") & fingerprint.ne("")
    if payee_fallback.any():
        out.loc[payee_fallback, "payee_selected"] = fingerprint.loc[payee_fallback]
        out.loc[payee_fallback, "payee_options"] = fingerprint.loc[payee_fallback]

    category_selected = _string_series(out, "category_selected")
    category_options = _string_series(out, "category_options")
    payee_selected = _string_series(out, "payee_selected")
    uncategorized = str(uncategorized_category or "").strip()
    if uncategorized:
        transfer = payee_selected.map(_is_transfer_payee)
        category_fallback = (
            eligible
            & category_selected.eq("")
            & category_options.eq("")
            & ~transfer
        )
        if category_fallback.any():
            out.loc[category_fallback, "category_selected"] = uncategorized
            out.loc[category_fallback, "category_options"] = uncategorized

    return out
