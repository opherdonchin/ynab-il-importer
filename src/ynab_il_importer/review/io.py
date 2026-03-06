from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from ynab_il_importer.review.validation import normalize_update_map


REQUIRED_COLUMNS = [
    "transaction_id",
    "date",
    "payee_options",
    "category_options",
    "payee_selected",
    "category_selected",
    "match_status",
    "update_map",
    "fingerprint",
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

    df["update_map"] = normalize_update_map(df["update_map"])
    return df


def save_reviewed_transactions(df: pd.DataFrame, path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    out = df.copy()
    if "update_map" in out.columns:
        out["update_map"] = out["update_map"].map(lambda v: "TRUE" if bool(v) else "")

    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    out.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(output_path)
