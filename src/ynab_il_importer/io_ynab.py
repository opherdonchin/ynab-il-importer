from pathlib import Path

import pandas as pd


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in normalized:
            return str(normalized[key])
    return None


def _series_or_default(df: pd.DataFrame, col: str | None, default: str = "") -> pd.Series:
    if col and col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def _parse_amount(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("")
    text = text.str.replace(",", "", regex=False)
    text = text.str.replace("₪", "", regex=False)
    text = text.str.replace(r"[^\d.\-()]", "", regex=True)
    text = text.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(text, errors="coerce").fillna(0.0)


def _direction_from_amount(amount: float) -> str:
    if amount > 0:
        return "inflow"
    if amount < 0:
        return "outflow"
    return "zero"


def read_ynab_register(path: str | Path) -> pd.DataFrame:
    raw = pd.read_csv(Path(path))
    raw.columns = [str(col).strip() for col in raw.columns]

    date_col = _find_column(raw, ["Date", "תאריך"])
    if date_col is None:
        raise ValueError("Could not find date column in YNAB register")

    payee_col = _find_column(raw, ["Payee", "מוטב", "שם מוטב"])
    category_col = _find_column(raw, ["Category", "קטגוריה", "Category Name"])
    master_col = _find_column(raw, ["Master Category", "קטגוריה ראשית"])
    sub_col = _find_column(raw, ["Sub Category", "Subcategory", "קטגוריית משנה"])
    outflow_col = _find_column(raw, ["Outflow", "הוצאה", "חיוב"])
    inflow_col = _find_column(raw, ["Inflow", "הכנסה", "זיכוי"])
    memo_col = _find_column(raw, ["Memo", "הערה", "הערות"])
    account_col = _find_column(raw, ["Account", "account"])

    if category_col:
        category = _series_or_default(raw, category_col).astype("string").fillna("")
    else:
        master = _series_or_default(raw, master_col).astype("string").fillna("").str.strip()
        sub = _series_or_default(raw, sub_col).astype("string").fillna("").str.strip()
        category = master.where(sub == "", master + ":" + sub).str.strip(":")

    outflow = _parse_amount(_series_or_default(raw, outflow_col, "0"))
    inflow = _parse_amount(_series_or_default(raw, inflow_col, "0"))

    result = pd.DataFrame(
        {
            "source": "ynab",
            "account_name": _series_or_default(raw, account_col).astype("string").fillna("").str.strip(),
            "date": pd.to_datetime(raw[date_col], errors="coerce", dayfirst=True).dt.date,
            "payee_raw": _series_or_default(raw, payee_col).astype("string").fillna(""),
            "category_raw": category,
            "outflow": outflow.round(2),
            "inflow": inflow.round(2),
            "memo": _series_or_default(raw, memo_col).astype("string").fillna(""),
            "currency": "ILS",
            "amount_bucket": "",
        }
    )
    result["amount_ils"] = (result["inflow"] - result["outflow"]).round(2)
    result["direction"] = result["amount_ils"].map(_direction_from_amount)
    result["txn_kind"] = result["direction"]

    return result[
        [
            "source",
            "account_name",
            "date",
            "payee_raw",
            "category_raw",
            "outflow",
            "inflow",
            "amount_ils",
            "direction",
            "txn_kind",
            "currency",
            "amount_bucket",
            "memo",
        ]
    ]
