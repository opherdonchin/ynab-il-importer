from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

import ynab_il_importer.review_app.validation as validation


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


def _missing_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    return [col for col in required if col not in df.columns]


def load_proposed_transactions(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing proposed transactions file: {csv_path}")

    df = pd.read_csv(csv_path, dtype="string").fillna("")
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


def save_reviewed_transactions(df: pd.DataFrame, path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    out = df.copy()
    if "payee_selected" in out.columns and "target_payee_selected" in out.columns:
        out["target_payee_selected"] = out["payee_selected"].astype("string").fillna("").str.strip()
    if "category_selected" in out.columns and "target_category_selected" in out.columns:
        out["target_category_selected"] = out["category_selected"].astype("string").fillna("").str.strip()
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


def load_category_list(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing categories file: {csv_path}")
    df = pd.read_csv(csv_path, dtype="string").fillna("")
    if "category_name" not in df.columns:
        raise ValueError("Categories file must contain a category_name column.")
    if "category_group" not in df.columns:
        df["category_group"] = ""
    return df[["category_group", "category_name"]].copy()
