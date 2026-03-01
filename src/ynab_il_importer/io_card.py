from pathlib import Path
from typing import Any
import re

import pandas as pd
from ynab_il_importer.account_map import apply_account_name_map
from ynab_il_importer.fingerprint import fingerprint_hash_v1
from ynab_il_importer.fingerprint import fingerprint_v0
from ynab_il_importer.normalize import normalize_text


HEADER_MARKER = "תאריך עסקה"
CARD_TXN_KIND = "card"
_NON_DIGIT_RE = re.compile(r"\D+")
_DIGITS_ONLY_RE = re.compile(r"^\d+$")
_DECIMAL_ZERO_RE = re.compile(r"^\d+\.0+$")


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).strip() for col in out.columns]
    return out


def _get_column(df: pd.DataFrame, name: str, default: Any = "") -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series([default] * len(df), index=df.index)


def _parse_amount(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("")
    text = text.str.replace(",", "", regex=False)
    text = text.str.replace("₪", "", regex=False)
    text = text.str.replace(r"[^\d.\-()]", "", regex=True)
    text = text.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(text, errors="coerce").fillna(0.0)


def _find_header(path: Path) -> tuple[str, int]:
    sheets = pd.read_excel(path, sheet_name=None, header=None, dtype=str)
    for sheet_name, sheet_df in sheets.items():
        matches = sheet_df.apply(
            lambda row: row.astype("string").fillna("").str.strip().eq(HEADER_MARKER).any(),
            axis=1,
        )
        if matches.any():
            return sheet_name, int(matches.idxmax())
    raise ValueError(f"Could not find header row containing '{HEADER_MARKER}' in {path}")


def _pick_amount_column(df: pd.DataFrame) -> str:
    candidates = ["סכום חיוב", "סכום עסקה", "סכום", "חיוב", 'סכום בש"ח', "סכום בשח"]
    for name in candidates:
        if name in df.columns:
            return name
    raise ValueError(f"Could not infer amount column in card file. Columns: {list(df.columns)}")


def _normalize_currency(series: pd.Series) -> pd.Series:
    out = series.astype("string").fillna("").str.strip()
    out = out.replace({"₪": "ILS", "ש\"ח": "ILS", "שח": "ILS"})
    out = out.str.upper()
    return out.where(out != "", "ILS")


def _normalize_card_account_name(series: pd.Series) -> pd.Series:
    def _extract_digits(value: object) -> str:
        text = str(value).strip()
        if text in {"", "nan", "NaN", "None"}:
            return ""
        if _DIGITS_ONLY_RE.match(text):
            return text
        if _DECIMAL_ZERO_RE.match(text):
            return text.split(".", 1)[0]
        return _NON_DIGIT_RE.sub("", text)

    digits = series.astype("string").fillna("").map(_extract_digits)
    valid = digits.str.len() >= 4
    out = pd.Series([""] * len(series), index=series.index, dtype="string")
    out.loc[valid] = "x" + digits.loc[valid].str[-4:]
    return out


def _pick_card_account_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "4 ספרות אחרונות של כרטיס האשראי",
        "4 ספרות אחרונות",
    ]
    for name in candidates:
        if name in df.columns:
            return name
    for col in df.columns:
        if "4 ספרות אחרונות" in str(col):
            return str(col)
    return None


def read_card(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    sheet_name, header_row = _find_header(path)
    raw = _clean_columns(pd.read_excel(path, sheet_name=sheet_name, header=header_row))

    amount_col = _pick_amount_column(raw)
    amount = _parse_amount(raw[amount_col])
    non_zero = amount[amount != 0]
    if not non_zero.empty and (non_zero > 0).mean() >= 0.8:
        amount = -amount.abs()

    merchant = _get_column(raw, "שם בית העסק", "").astype("string").fillna("").str.strip()
    notes = _get_column(raw, "הערות", "").astype("string").fillna("").str.strip()
    description = merchant.where(notes == "", merchant + " | " + notes).str.strip(" |")
    description_clean = description.where(description != "", merchant).astype("string").fillna("").str.strip()
    description_clean_norm = description_clean.map(normalize_text)
    fingerprint = description_clean_norm.map(fingerprint_v0)
    fingerprint_hash = [
        fingerprint_hash_v1(CARD_TXN_KIND, description_norm)
        for description_norm in description_clean_norm.tolist()
    ]

    account_col = _pick_card_account_column(raw)
    if account_col is not None:
        account_name = _normalize_card_account_name(raw[account_col])
    else:
        account_name = pd.Series([""] * len(raw), index=raw.index, dtype="string")

    result = pd.DataFrame(
        {
            "source": "card",
            "account_name": account_name,
            "source_account": account_name,
            "date": pd.to_datetime(
                _get_column(raw, "תאריך עסקה", None), errors="coerce", dayfirst=True
            ).dt.date,
            "charge_date": pd.to_datetime(
                _get_column(raw, "תאריך חיוב", None), errors="coerce", dayfirst=True
            ).dt.date,
            "txn_kind": CARD_TXN_KIND,
            "merchant_raw": merchant,
            "description_raw": description,
            "description_clean": description_clean,
            "description_clean_norm": description_clean_norm,
            "fingerprint": fingerprint,
            "fingerprint_hash": pd.Series(fingerprint_hash, index=raw.index, dtype="string"),
            "amount_ils": amount.round(2),
            "currency": _normalize_currency(_get_column(raw, "מטבע חיוב", "")),
        }
    )

    # Drop pure empty noise rows often present in report footers.
    result = result[
        result["date"].notna()
        | (result["description_raw"].astype("string").fillna("").str.strip() != "")
        | (result["merchant_raw"].astype("string").fillna("").str.strip() != "")
        | (result["amount_ils"] != 0)
    ]
    result = apply_account_name_map(result, source="card")

    return result[
        [
            "source",
            "account_name",
            "source_account",
            "date",
            "charge_date",
            "txn_kind",
            "merchant_raw",
            "description_raw",
            "description_clean",
            "description_clean_norm",
            "fingerprint",
            "fingerprint_hash",
            "amount_ils",
            "currency",
        ]
    ]
