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


def _infer_txn_kind(
    inflow_ils: pd.Series, outflow_ils: pd.Series, payee_raw: pd.Series, category_raw: pd.Series
) -> pd.Series:
    inflow = pd.to_numeric(inflow_ils, errors="coerce").fillna(0.0)
    outflow = pd.to_numeric(outflow_ils, errors="coerce").fillna(0.0)
    payee = payee_raw.astype("string").fillna("").str.strip().str.lower()
    category = category_raw.astype("string").fillna("").str.strip().str.lower()

    kind = pd.Series(["expense"] * len(inflow), index=inflow.index, dtype="string")
    is_transfer = payee.str.startswith("transfer :") | payee.str.startswith("transfer:")
    kind.loc[is_transfer] = "transfer"

    is_inflow = inflow > 0
    is_income = is_inflow & category.str.contains("ready to assign", regex=False)
    kind.loc[is_income] = "income"
    kind.loc[is_inflow & ~is_income & ~is_transfer] = "credit"
    return kind


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

    outflow_ils = _parse_amount(_series_or_default(raw, outflow_col, "0")).round(2)
    inflow_ils = _parse_amount(_series_or_default(raw, inflow_col, "0")).round(2)

    result = pd.DataFrame(
        {
            "source": "ynab",
            "account_name": _series_or_default(raw, account_col).astype("string").fillna("").str.strip(),
            "date": pd.to_datetime(raw[date_col], errors="coerce", dayfirst=True).dt.date,
            "payee_raw": _series_or_default(raw, payee_col).astype("string").fillna(""),
            "category_raw": category,
            "outflow_ils": outflow_ils,
            "inflow_ils": inflow_ils,
            "memo": _series_or_default(raw, memo_col).astype("string").fillna(""),
            "currency": "ILS",
            "amount_bucket": "",
        }
    )
    result["txn_kind"] = _infer_txn_kind(
        result["inflow_ils"],
        result["outflow_ils"],
        result["payee_raw"],
        result["category_raw"],
    )

    return result[
        [
            "source",
            "account_name",
            "date",
            "payee_raw",
            "category_raw",
            "outflow_ils",
            "inflow_ils",
            "txn_kind",
            "currency",
            "amount_bucket",
            "memo",
        ]
    ]
