from __future__ import annotations

from typing import Any

import pandas as pd


def parse_option_string(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    text = str(value).strip()
    if not text:
        return []
    parts = [part.strip() for part in text.split(";")]
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        if not part or part in seen:
            continue
        ordered.append(part)
        seen.add(part)
    return ordered


def resolve_selected_value(selected_value: Any, override_value: Any) -> str:
    override = "" if override_value is None else str(override_value).strip()
    if override:
        return override
    return "" if selected_value is None else str(selected_value).strip()


def apply_to_same_fingerprint(
    df: pd.DataFrame,
    fingerprint: str,
    payee: str | None = None,
    category: str | None = None,
    update_map: bool | None = None,
    reviewed: bool | None = None,
    eligible_mask: pd.Series | None = None,
) -> pd.DataFrame:
    mask = df["fingerprint"].astype("string").fillna("").str.strip() == str(fingerprint).strip()
    if eligible_mask is not None:
        eligible = eligible_mask.reindex(df.index, fill_value=False).astype(bool)
        mask = mask & eligible
    if payee is not None:
        df.loc[mask, "payee_selected"] = payee
    if category is not None:
        df.loc[mask, "category_selected"] = category
    if update_map is not None:
        df.loc[mask, "update_map"] = bool(update_map)
    if reviewed is not None:
        df.loc[mask, "reviewed"] = bool(reviewed)
    return df
