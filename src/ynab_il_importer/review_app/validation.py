from __future__ import annotations

from typing import Any

import pandas as pd

import ynab_il_importer.review_app.model as model


TRUE_VALUES = {"1", "true", "t", "yes", "y"}


def normalize_update_map(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip().str.lower()
    return text.isin(TRUE_VALUES)


def validate_row(row: pd.Series) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    payee = str(row.get("payee_selected", "") or "").strip()
    category = str(row.get("category_selected", "") or "").strip()
    update_map = bool(row.get("update_map", False))
    category_required = not model.is_transfer_payee(payee)

    if not payee:
        errors.append("missing payee")
    if category_required and not category:
        errors.append("missing category")

    if ";" in payee:
        warnings.append("payee contains ';'")
    if ";" in category:
        warnings.append("category contains ';'")

    if update_map and (not payee or (category_required and not category)):
        warnings.append("update_map set while payee/category missing")

    payee_options = model.parse_option_string(row.get("payee_options", ""))
    category_options = model.parse_option_string(row.get("category_options", ""))
    if payee and payee_options and payee not in payee_options:
        warnings.append("payee not in options")
    if category and category_options and category not in category_options:
        warnings.append("category not in options")

    return errors, warnings


def inconsistent_fingerprints(df: pd.DataFrame) -> pd.DataFrame:
    payee = df["payee_selected"].astype("string").fillna("").str.strip()
    category = df["category_selected"].astype("string").fillna("").str.strip()
    key = payee + "||" + category
    grouped = (
        df.assign(_combo=key)
        .groupby("fingerprint", dropna=False)["_combo"]
        .nunique()
        .reset_index(name="combo_count")
    )
    return grouped[grouped["combo_count"] > 1]
