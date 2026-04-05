from __future__ import annotations

import json
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa

import ynab_il_importer.review_app.model as model
from ynab_il_importer.artifacts.transaction_schema import SPLIT_LINE_STRUCT, TRANSACTION_SCHEMA
from ynab_il_importer.safe_types import normalize_flag_series


SPLIT_FIELD_NAMES = [field.name for field in SPLIT_LINE_STRUCT]
TRANSACTION_FIELD_NAMES = [field.name for field in TRANSACTION_SCHEMA]
SPLIT_COLUMNS = ["source_splits", "target_splits"]
CURRENT_TRANSACTION_COLUMNS = [
    "source_current_transaction",
    "target_current_transaction",
]
ORIGINAL_TRANSACTION_COLUMNS = [
    "source_original_transaction",
    "target_original_transaction",
]
WORKING_REQUIRED_COLUMNS = [
    "transaction_id",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "source_present",
    "target_present",
]
WORKING_INPUT_REQUIRED_COLUMNS = [
    "source_present",
    "target_present",
]
WORKING_COLUMNS = [
    "transaction_id",
    "source",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "fingerprint",
    "workflow_type",
    "relation_kind",
    "match_status",
    "match_method",
    "payee_options",
    "category_options",
    "update_maps",
    "decision_action",
    "reviewed",
    "changed",
    "memo_append",
    "source_present",
    "target_present",
    "source_row_id",
    "target_row_id",
    "target_account",
    "source_context_kind",
    "source_context_category_id",
    "source_context_category_name",
    "source_context_matching_split_ids",
    "target_context_kind",
    "target_context_matching_split_ids",
    *[
        "source_source_system",
        "source_transaction_id",
        "source_ynab_id",
        "source_import_id",
        "source_parent_transaction_id",
        "source_account_id",
        "source_account",
        "source_date",
        "source_secondary_date",
        "source_payee_current",
        "source_category_id",
        "source_category_current",
        "source_memo",
        "source_fingerprint",
        "source_description_raw",
        "source_description_clean",
        "source_description_clean_norm",
        "source_merchant_raw",
        "source_ref",
        "source_matched_transaction_id",
        "source_cleared",
        "source_approved",
        "source_is_subtransaction",
        "target_source_system",
        "target_transaction_id",
        "target_ynab_id",
        "target_import_id",
        "target_parent_transaction_id",
        "target_account_id",
        "target_account",
        "target_date",
        "target_secondary_date",
        "target_payee_current",
        "target_category_id",
        "target_category_current",
        "target_memo",
        "target_fingerprint",
        "target_description_raw",
        "target_description_clean",
        "target_description_clean_norm",
        "target_merchant_raw",
        "target_ref",
        "target_matched_transaction_id",
        "target_cleared",
        "target_approved",
        "target_is_subtransaction",
        "source_splits",
        "target_splits",
        "source_payee_selected",
        "source_category_selected",
        "target_payee_selected",
        "target_category_selected",
        "payee_selected",
        "category_selected",
    ],
    *CURRENT_TRANSACTION_COLUMNS,
    *ORIGINAL_TRANSACTION_COLUMNS,
]
WORKING_TEXT_COLUMNS = [
    column
    for column in WORKING_COLUMNS
    if column
    not in {
        "outflow_ils",
        "inflow_ils",
        "reviewed",
        "changed",
        "source_present",
        "target_present",
        "source_approved",
        "source_is_subtransaction",
        "target_approved",
        "target_is_subtransaction",
        *SPLIT_COLUMNS,
        *CURRENT_TRANSACTION_COLUMNS,
        *ORIGINAL_TRANSACTION_COLUMNS,
    }
]
WORKING_BOOL_COLUMNS = [
    "reviewed",
    "changed",
    "source_present",
    "target_present",
    "source_approved",
    "source_is_subtransaction",
    "target_approved",
    "target_is_subtransaction",
]


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _normalize_float(value: Any) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])


def _normalize_split_records(value: Any) -> list[dict[str, Any]] | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            return None
    if not isinstance(value, list):
        if isinstance(value, dict):
            return None
        try:
            value = list(value)
        except TypeError:
            return None
    normalized: list[dict[str, Any]] = []
    for raw in value:
        if not isinstance(raw, dict):
            continue
        normalized.append(
            {
                name: (
                    _normalize_float(raw.get(name))
                    if name in {"inflow_ils", "outflow_ils"}
                    else _normalize_text(raw.get(name))
                )
                for name in SPLIT_FIELD_NAMES
            }
        )
    return normalized or None


def _normalize_transaction_record(value: Any) -> dict[str, Any] | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            return None
    if not isinstance(value, dict):
        return None

    normalized: dict[str, Any] = {}
    for field in TRANSACTION_SCHEMA:
        raw = value.get(field.name)
        if raw is None or raw is pd.NA:
            normalized[field.name] = None if pa.types.is_list(field.type) else ""
            if pa.types.is_boolean(field.type):
                normalized[field.name] = False
            elif pa.types.is_floating(field.type):
                normalized[field.name] = 0.0
            continue
        if pa.types.is_boolean(field.type):
            normalized[field.name] = bool(normalize_flag_series(pd.Series([raw])).iloc[0])
        elif pa.types.is_floating(field.type):
            normalized[field.name] = _normalize_float(raw)
        elif pa.types.is_list(field.type):
            normalized[field.name] = _normalize_split_records(raw)
        else:
            normalized[field.name] = _normalize_text(raw)
    return normalized


def decode_working_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in SPLIT_COLUMNS:
        if column in out.columns:
            out[column] = out[column].map(_normalize_split_records)
    for column in CURRENT_TRANSACTION_COLUMNS:
        if column in out.columns:
            out[column] = out[column].map(_normalize_transaction_record)
    for column in ORIGINAL_TRANSACTION_COLUMNS:
        if column in out.columns:
            out[column] = out[column].map(_normalize_transaction_record)
    return out


def missing_working_columns(df: pd.DataFrame, required: Iterable[str] | None = None) -> list[str]:
    needed = list(WORKING_REQUIRED_COLUMNS if required is None else required)
    return [column for column in needed if column not in df.columns]


def validate_working_dataframe(df: pd.DataFrame) -> None:
    missing = missing_working_columns(df)
    if missing:
        raise ValueError(f"Review working rows missing required columns: {missing}")


def _working_default(column: str) -> Any:
    if column in {"outflow_ils", "inflow_ils"}:
        return 0.0
    if column in WORKING_BOOL_COLUMNS:
        return False
    if (
        column in SPLIT_COLUMNS
        or column in CURRENT_TRANSACTION_COLUMNS
        or column in ORIGINAL_TRANSACTION_COLUMNS
    ):
        return None
    return ""


def build_working_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = decode_working_dataframe(df.copy())
    missing_input = missing_working_columns(out, WORKING_INPUT_REQUIRED_COLUMNS)
    if missing_input:
        raise ValueError(f"Review working rows missing required columns: {missing_input}")

    for column in WORKING_COLUMNS:
        if column not in out.columns:
            out[column] = _working_default(column)

    if "source" in out.columns and "workflow_type" in out.columns:
        institutional_mask = out["workflow_type"].astype("string").fillna("").str.strip().eq("")
        source_series = out["source"].astype("string").fillna("").str.strip().str.casefold()
        out.loc[institutional_mask & source_series.isin(["bank", "card"]), "workflow_type"] = (
            "institutional"
        )

    for column in WORKING_BOOL_COLUMNS:
        out[column] = normalize_flag_series(out[column])

    out["update_maps"] = out["update_maps"].astype("string").fillna("").str.strip()
    out["decision_action"] = out["decision_action"].astype("string").fillna("").str.strip()

    for column in WORKING_TEXT_COLUMNS:
        out[column] = out[column].astype("string").fillna("").str.strip()

    if "payee_selected" in out.columns and "target_payee_selected" in out.columns:
        out["target_payee_selected"] = out["target_payee_selected"].where(
            out["target_payee_selected"].ne(""),
            out["payee_selected"],
        )
    if "category_selected" in out.columns and "target_category_selected" in out.columns:
        out["target_category_selected"] = out["target_category_selected"].where(
            out["target_category_selected"].ne(""),
            out["category_selected"],
        )

    target_payee = out["target_payee_selected"]
    target_category = out["target_category_selected"].map(model.normalize_category_value)
    out["payee_selected"] = target_payee.where(target_payee.ne(""), out["payee_selected"])
    out["category_selected"] = target_category.where(target_category.ne(""), out["category_selected"])
    out["target_category_selected"] = target_category
    out["source_category_selected"] = out["source_category_selected"].map(
        model.normalize_category_value
    )
    out["target_category_current"] = out["target_category_current"].map(model.normalize_category_value)
    out["source_category_current"] = out["source_category_current"].map(model.normalize_category_value)

    ordered_columns = list(
        dict.fromkeys(WORKING_COLUMNS + [column for column in out.columns if column not in WORKING_COLUMNS])
    )
    out = out[ordered_columns].copy()
    validate_working_dataframe(out)
    return out
