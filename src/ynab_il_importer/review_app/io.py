from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import polars as pl
import pyarrow as pa

import ynab_il_importer.review_app.validation as validation
import ynab_il_importer.review_app.model as model


REQUIRED_COLUMNS = [
    "transaction_id",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "payee_options",
    "category_options",
    "match_status",
    "update_maps",
    "decision_action",
    "fingerprint",
    "workflow_type",
    "source_payee_selected",
    "source_category_selected",
    "target_payee_selected",
    "target_category_selected",
]

LEGACY_INSTITUTIONAL_REQUIRED_COLUMNS = [
    "transaction_id",
    "source",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "fingerprint",
    "payee_options",
    "category_options",
    "payee_selected",
    "category_selected",
    "match_status",
    "reviewed",
]


def _missing_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    return [col for col in required if col not in df.columns]


def _text_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    return df[column].astype("string").fillna("").str.strip()


def _legacy_source_row_ids(df: pd.DataFrame) -> pd.Series:
    source = _text_series(df, "source").str.casefold()
    bank_ids = _text_series(df, "bank_txn_id")
    card_ids = _text_series(df, "card_txn_id")
    row_ids = pd.Series([""] * len(df), index=df.index, dtype="string")
    row_ids = row_ids.mask(source.eq("bank"), bank_ids)
    row_ids = row_ids.mask(source.eq("card"), card_ids)
    return row_ids


def _legacy_update_maps(df: pd.DataFrame) -> pd.Series:
    if "update_map" not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    flagged = validation.normalize_flag_series(df["update_map"])
    return pd.Series(
        ["payee_add_fingerprint" if bool(value) else "" for value in flagged],
        index=df.index,
        dtype="string",
    )


def _legacy_institutional_mask(df: pd.DataFrame) -> bool:
    if _missing_columns(df, LEGACY_INSTITUTIONAL_REQUIRED_COLUMNS):
        return False
    sources = set(_text_series(df, "source").str.casefold().tolist()) - {""}
    return bool(sources) and sources <= {"bank", "card"}


def _translate_legacy_institutional_review(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["legacy_review_schema"] = "legacy_institutional_v0"
    out["legacy_match_status"] = _text_series(out, "match_status")
    out["match_status"] = "source_only"
    out["update_maps"] = _legacy_update_maps(out)
    out["decision_action"] = "create_target"
    out["workflow_type"] = "institutional"
    out["source_payee_selected"] = ""
    out["source_category_selected"] = ""
    out["target_payee_selected"] = _text_series(out, "payee_selected")
    out["target_category_selected"] = _text_series(out, "category_selected")
    out["source_present"] = True
    out["target_present"] = False
    out["source_row_id"] = _legacy_source_row_ids(out)
    out["target_row_id"] = ""
    out["target_account"] = _text_series(out, "account_name")
    out["source_date"] = _text_series(out, "date")
    out["target_date"] = ""
    out["source_memo"] = _text_series(out, "memo")
    out["target_memo"] = ""
    out["source_fingerprint"] = _text_series(out, "fingerprint")
    out["target_fingerprint"] = ""
    return out


def detect_review_csv_format(df: pd.DataFrame) -> str:
    if not _missing_columns(df, REQUIRED_COLUMNS):
        return "unified_v1"
    if _legacy_institutional_mask(df):
        return "legacy_institutional_v0"
    return "unknown"


def translate_review_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    fmt = detect_review_csv_format(df)
    if fmt == "unified_v1":
        return df.copy()
    if fmt == "legacy_institutional_v0":
        return _translate_legacy_institutional_review(df)
    raise ValueError("Unsupported review CSV format for translation.")


def _input_to_pandas_dataframe(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
    *,
    label: str,
) -> pd.DataFrame:
    if isinstance(source, pd.DataFrame):
        return source.copy()
    if isinstance(source, pl.DataFrame):
        return source.to_pandas()
    if isinstance(source, pa.Table):
        return source.to_pandas()

    csv_path = Path(source)
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing {label} file: {csv_path}")
    return pd.read_csv(csv_path, dtype="string").fillna("")


def load_proposed_transactions(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pd.DataFrame:
    df = _input_to_pandas_dataframe(source, label="proposed transactions")
    detected_format = detect_review_csv_format(df)
    if detected_format != "unified_v1":
        if detected_format.startswith("legacy_"):
            raise ValueError(
                "proposed_transactions is in legacy review format "
                f"({detected_format}); run scripts/translate_review_csv.py first"
            )
        raise ValueError(
            f"proposed_transactions missing columns: {_missing_columns(df, REQUIRED_COLUMNS)}"
        )
    missing = _missing_columns(df, REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"proposed_transactions missing columns: {missing}")

    for col in [
        "payee_options",
        "category_options",
        "source_payee_selected",
        "source_category_selected",
        "target_payee_selected",
        "target_category_selected",
        "match_status",
        "fingerprint",
        "workflow_type",
    ]:
        df[col] = df[col].astype("string").fillna("").str.strip()
    for col in ["source_category_selected", "target_category_selected"]:
        if col in df.columns:
            df[col] = df[col].map(model.normalize_category_value)
    if "memo_append" not in df.columns:
        df["memo_append"] = ""
    df["memo_append"] = df["memo_append"].astype("string").fillna("").str.strip()

    df["update_maps"] = validation.normalize_update_maps(df["update_maps"])
    if "reviewed" not in df.columns:
        df["reviewed"] = False
    else:
        df["reviewed"] = validation.normalize_flag_series(df["reviewed"])
    if "source_present" in df.columns:
        df["source_present"] = validation.normalize_flag_series(df["source_present"])
    if "target_present" in df.columns:
        df["target_present"] = validation.normalize_flag_series(df["target_present"])
    df["decision_action"] = validation.normalize_decision_actions(df["decision_action"])
    df["payee_selected"] = df["target_payee_selected"]
    df["category_selected"] = df["target_category_selected"]
    return df


def save_reviewed_transactions(
    df: pd.DataFrame | pl.DataFrame | pa.Table,
    path: str | Path,
) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    out = _input_to_pandas_dataframe(df, label="reviewed transactions")
    if "target_payee_selected" not in out.columns and "payee_selected" in out.columns:
        out["target_payee_selected"] = out["payee_selected"].astype("string").fillna("").str.strip()
    if "target_category_selected" not in out.columns and "category_selected" in out.columns:
        out["target_category_selected"] = (
            out["category_selected"].astype("string").fillna("").str.strip()
        )
    out = out.drop(
        columns=[col for col in ["payee_selected", "category_selected"] if col in out.columns]
    )
    if "update_maps" in out.columns:
        out["update_maps"] = validation.normalize_update_maps(out["update_maps"])
    if "reviewed" in out.columns:
        out["reviewed"] = out["reviewed"].map(lambda v: "TRUE" if bool(v) else "")
    for flag_col in ["source_present", "target_present"]:
        if flag_col in out.columns:
            out[flag_col] = out[flag_col].map(lambda v: "TRUE" if bool(v) else "")

    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    out.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(output_path)


def load_category_list(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pd.DataFrame:
    df = _input_to_pandas_dataframe(source, label="categories")
    if "category_name" not in df.columns:
        raise ValueError("Categories file must contain a category_name column.")
    if "category_group" not in df.columns:
        df["category_group"] = ""
    return df[["category_group", "category_name"]].copy()
