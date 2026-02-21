from pathlib import Path
from typing import Any
from io import StringIO


import pandas as pd
from lxml import html

_BANK_REQUIRED_HEADERS = {"תאריך", "תיאור", "בחובה", "בזכות"}


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).strip() for col in out.columns]
    return out


def _normalize_cell(value: Any) -> str:
    text = str(value).strip().replace("\xa0", " ")
    if text in {"nan", "NaN", "None"}:
        return ""
    return text


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


def _looks_like_transaction_table(df: pd.DataFrame) -> bool:
    cols = set(_clean_columns(df).columns)
    if _BANK_REQUIRED_HEADERS.issubset(cols):
        return True

    norm = df.applymap(_normalize_cell)
    for _, row in norm.iterrows():
        vals = {v for v in row.tolist() if v}
        if _BANK_REQUIRED_HEADERS.issubset(vals):
            return True
    return False


def _promote_transaction_header(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    tmp.columns = [f"col_{i}" for i in range(tmp.shape[1])]
    norm = tmp.applymap(_normalize_cell)

    header_idx: int | None = None
    for i, row in norm.iterrows():
        vals = {v for v in row.tolist() if v}
        if _BANK_REQUIRED_HEADERS.issubset(vals):
            header_idx = int(i)
            break

    if header_idx is None:
        return _clean_columns(df)

    headers = norm.iloc[header_idx].tolist()
    headers = [h if h else f"unnamed_{i}" for i, h in enumerate(headers)]

    body = norm.iloc[header_idx + 1 :].copy()
    body.columns = headers
    body = body.loc[:, ~pd.Index(body.columns).duplicated(keep="first")]
    body = body[
        body.apply(lambda r: any(str(v).strip() != "" for v in r.tolist()), axis=1)
    ]
    return _clean_columns(body.reset_index(drop=True))


def _select_bank_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    candidates = [
        _promote_transaction_header(t)
        for t in tables
        if _looks_like_transaction_table(t)
    ]
    candidates = [
        t for t in candidates if _BANK_REQUIRED_HEADERS.issubset(set(t.columns))
    ]
    if candidates:
        return max(candidates, key=len)
    if tables:
        return _promote_transaction_header(tables[0])
    return None


def _extract_bank_table_with_lxml(path: Path) -> pd.DataFrame | None:
    """Fallback extractor for HTML exports where pandas misses nested tables."""
    try:
        doc = html.fromstring(path.read_bytes())
    except Exception:
        return None

    best_df: pd.DataFrame | None = None

    for table in doc.xpath("//table"):
        rows: list[list[str]] = []
        for tr in table.xpath(".//tr"):
            cells = tr.xpath("./th|./td")
            if not cells:
                continue
            values = [
                _normalize_cell(" ".join(" ".join(cell.itertext()).split())) for cell in cells
            ]
            rows.append(values)

        if not rows:
            continue

        for i, row in enumerate(rows):
            header_values = {value for value in row if value}
            if not _BANK_REQUIRED_HEADERS.issubset(header_values):
                continue

            header = [value if value else f"unnamed_{idx}" for idx, value in enumerate(row)]
            width = len(header)

            data_rows: list[list[str]] = []
            for data_row in rows[i + 1 :]:
                if len(data_row) == width:
                    data_rows.append(data_row)

            if not data_rows:
                continue

            candidate = pd.DataFrame(data_rows, columns=header)
            candidate = candidate[
                candidate.apply(
                    lambda r: any(str(v).strip() not in {"", "nan", "NaN", "None"} for v in r),
                    axis=1,
                )
            ].reset_index(drop=True)

            if not _BANK_REQUIRED_HEADERS.issubset(set(candidate.columns)):
                continue

            if best_df is None or len(candidate) > len(best_df):
                best_df = candidate

            break

    return _clean_columns(best_df) if best_df is not None else None


def _read_bank_table(path: Path) -> pd.DataFrame:
    # 1) HTML first (these bank ".xls" files are often HTML)
    html_loaders = [
        lambda: pd.read_html(path),
        lambda: pd.read_html(path, header=None),
    ]

    # Also try parsing from text buffer (works better on some malformed exports)
    text = path.read_text(encoding="utf-8", errors="ignore")
    html_loaders.extend(
        [
            lambda: pd.read_html(StringIO(text)),
            lambda: pd.read_html(StringIO(text), header=None),
        ]
    )

    for load in html_loaders:
        try:
            tables = load()
            selected = _select_bank_table(tables)
            if selected is not None:
                return selected
        except Exception:
            continue

    lxml_selected = _extract_bank_table_with_lxml(path)
    if lxml_selected is not None:
        return lxml_selected

    # 2) True Excel fallback (explicit engines)
    for engine in ("xlrd", "openpyxl"):
        try:
            return _promote_transaction_header(
                pd.read_excel(path, header=None, engine=engine)
            )
        except Exception:
            continue

    raise ValueError(f"Could not parse bank file as HTML or Excel: {path}")


def read_bank(path: str | Path) -> pd.DataFrame:
    raw = _read_bank_table(Path(path))

    outflow = _parse_amount(_get_column(raw, "בחובה", 0.0))
    inflow = _parse_amount(_get_column(raw, "בזכות", 0.0))

    result = pd.DataFrame(
        {
            "source": "bank",
            "date": pd.to_datetime(
                _get_column(raw, "תאריך"), errors="coerce", dayfirst=True
            ).dt.date,
            "value_date": pd.to_datetime(
                _get_column(raw, "תאריך ערך", None), errors="coerce", dayfirst=True
            ).dt.date,
            "description_raw": _get_column(raw, "תיאור", "")
            .astype("string")
            .fillna(""),
            "ref": _get_column(raw, "אסמכתא", "").astype("string").fillna(""),
            "outflow_ils": outflow,
            "inflow_ils": inflow,
        }
    )
    result["amount_ils"] = (result["inflow_ils"] - result["outflow_ils"]).round(2)

    # Drop pure empty noise rows.
    result = result[
        result["date"].notna()
        | (result["description_raw"].str.strip() != "")
        | (result["outflow_ils"] != 0)
        | (result["inflow_ils"] != 0)
    ]

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
