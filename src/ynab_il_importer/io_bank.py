from pathlib import Path
from typing import Any

import pandas as pd


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


def _read_bank_table(path: Path) -> pd.DataFrame:
    try:
        tables = pd.read_html(path)
        cleaned = [_clean_columns(table) for table in tables]
        for table in cleaned:
            if "תאריך" in table.columns:
                return table
        if cleaned:
            return cleaned[0]
    except ValueError:
        pass
    except Exception:
        pass

    return _clean_columns(pd.read_excel(path))


def read_bank(path: str | Path) -> pd.DataFrame:
    raw = _read_bank_table(Path(path))

    outflow = _parse_amount(_get_column(raw, "בחובה", 0.0))
    inflow = _parse_amount(_get_column(raw, "בזכות", 0.0))

    result = pd.DataFrame(
        {
            "source": "bank",
            "date": pd.to_datetime(_get_column(raw, "תאריך"), errors="coerce", dayfirst=True).dt.date,
            "value_date": pd.to_datetime(
                _get_column(raw, "תאריך ערך", None), errors="coerce", dayfirst=True
            ).dt.date,
            "description_raw": _get_column(raw, "תיאור", "").astype("string").fillna(""),
            "ref": _get_column(raw, "אסמכתא", "").astype("string").fillna(""),
            "outflow_ils": outflow,
            "inflow_ils": inflow,
        }
    )
    result["amount_ils"] = (result["inflow_ils"] - result["outflow_ils"]).round(2)

    return result[
        [
            "source",
            "date",
            "value_date",
            "description_raw",
            "ref",
            "outflow_ils",
            "inflow_ils",
            "amount_ils",
        ]
    ]
