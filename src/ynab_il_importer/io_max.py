from pathlib import Path
from typing import Any
import re

import pandas as pd
import ynab_il_importer.account_map as account_map
import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.fingerprint as fingerprint


HEADER_MARKER = "תאריך עסקה"
_NON_DIGIT_RE = re.compile(r"\D+")
_DIGITS_ONLY_RE = re.compile(r"^\d+$")
_DECIMAL_ZERO_RE = re.compile(r"^\d+\.0+$")
_WHITESPACE_RE = re.compile(r"\s+")


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


def _parse_optional_amount(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("")
    text = text.str.replace(",", "", regex=False)
    text = text.str.replace("₪", "", regex=False)
    text = text.str.replace(r"[^\d.\-()]", "", regex=True)
    text = text.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    text = text.where(text.str.strip() != "", pd.NA)
    return pd.to_numeric(text, errors="coerce")


def _load_workbook(path: Path) -> dict[str, pd.DataFrame]:
    return pd.read_excel(path, sheet_name=None, header=None, dtype=str)


def _find_headers(
    workbook: dict[str, pd.DataFrame], path: Path
) -> list[tuple[str, int]]:
    header_matches: list[tuple[str, int]] = []
    for sheet_name, sheet_df in workbook.items():
        row_matches = sheet_df.apply(
            lambda row: row.astype("string")
            .fillna("")
            .str.strip()
            .eq(HEADER_MARKER)
            .any(),
            axis=1,
        )
        if row_matches.any():
            header_matches.append((sheet_name, int(row_matches.idxmax())))
    if not header_matches:
        raise ValueError(
            f"Could not find header row containing '{HEADER_MARKER}' in {path}"
        )
    return header_matches


def _sheet_preface_value(sheet_df: pd.DataFrame, row_idx: int) -> str:
    if row_idx < 0 or row_idx >= len(sheet_df):
        return ""
    values = (
        sheet_df.iloc[row_idx]
        .astype("string")
        .fillna("")
        .map(lambda value: str(value).strip())
        .tolist()
    )
    text = " ".join(value for value in values if value)
    return _WHITESPACE_RE.sub(" ", text).strip()


def _extract_sheet_table(sheet_df: pd.DataFrame, header_row: int) -> pd.DataFrame:
    header = (
        sheet_df.iloc[header_row]
        .astype("string")
        .fillna("")
        .map(lambda value: str(value).strip())
        .tolist()
    )
    header = [value if value else f"unnamed_{idx}" for idx, value in enumerate(header)]
    body = sheet_df.iloc[header_row + 1 :].copy().reset_index(drop=True)
    body.columns = header
    body = body.loc[:, ~pd.Index(body.columns).duplicated(keep="first")]
    body = body[
        body.apply(
            lambda row: any(
                str(value).strip() not in {"", "nan", "NaN", "None"} for value in row
            ),
            axis=1,
        )
    ].reset_index(drop=True)
    return _clean_columns(body)


def _pick_amount_column(df: pd.DataFrame) -> str:
    candidates = ["סכום חיוב", "סכום עסקה", "סכום", "חיוב", 'סכום בש"ח', "סכום בשח"]
    for name in candidates:
        if name in df.columns:
            return name
    raise ValueError(
        f"Could not infer amount column in card file. Columns: {list(df.columns)}"
    )


def _normalize_currency(series: pd.Series) -> pd.Series:
    out = series.astype("string").fillna("").str.strip()
    out = out.replace({"₪": "ILS", 'ש"ח': "ILS", "שח": "ILS"})
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
    valid = digits.str.len() >= 3
    out = pd.Series([""] * len(series), index=series.index, dtype="string")
    out.loc[valid] = "x" + digits.loc[valid].str[-4:].str.zfill(4)
    return out


def _extract_card_suffix(series: pd.Series) -> pd.Series:
    account_name = _normalize_card_account_name(series)
    return (
        account_name.str.replace(r"^[xX]", "", regex=True).astype("string").fillna("")
    )


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


def _infer_txn_kind(inflow_ils: pd.Series, outflow_ils: pd.Series) -> pd.Series:
    inflow = pd.to_numeric(inflow_ils, errors="coerce").fillna(0.0)
    outflow = pd.to_numeric(outflow_ils, errors="coerce").fillna(0.0)
    kind = pd.Series(["expense"] * len(inflow), index=inflow.index, dtype="string")
    kind.loc[inflow > 0] = "credit"
    return kind


def _build_sheet_result(
    raw: pd.DataFrame,
    *,
    sheet_name: str,
    report_owner: str,
    report_scope: str,
    report_period: str,
) -> pd.DataFrame:
    amount_col = _pick_amount_column(raw)
    amount = _parse_amount(raw[amount_col])
    # MAX "charge amount" columns encode charges as positive and
    # refunds/reversals as negative. Convert to YNAB signed convention:
    # outflow as negative, inflow as positive.
    amount = -amount

    merchant = (
        _get_column(raw, "שם בית העסק", "").astype("string").fillna("").str.strip()
    )
    notes = _get_column(raw, "הערות", "").astype("string").fillna("").str.strip()
    description = merchant.where(notes == "", merchant + " | " + notes).str.strip(" |")
    description_clean = (
        description.where(description != "", merchant)
        .astype("string")
        .fillna("")
        .str.strip()
    )
    amount = amount.round(2)
    outflow_ils = amount.where(amount < 0, 0.0).abs().round(2)
    inflow_ils = amount.where(amount > 0, 0.0).round(2)
    txn_kind = _infer_txn_kind(inflow_ils, outflow_ils)

    account_col = _pick_card_account_column(raw)
    if account_col is not None:
        account_name = _normalize_card_account_name(raw[account_col])
        card_suffix = _extract_card_suffix(raw[account_col])
    else:
        account_name = pd.Series([""] * len(raw), index=raw.index, dtype="string")
        card_suffix = pd.Series([""] * len(raw), index=raw.index, dtype="string")

    return pd.DataFrame(
        {
            "source": "card",
            "account_name": account_name,
            "source_account": account_name,
            "card_suffix": card_suffix,
            "date": pd.to_datetime(
                _get_column(raw, "תאריך עסקה", None), errors="coerce", dayfirst=True
            ).dt.date,
            "secondary_date": pd.to_datetime(
                _get_column(raw, "תאריך חיוב", None), errors="coerce", dayfirst=True
            ).dt.date,
            "txn_kind": txn_kind,
            "merchant_raw": merchant,
            "description_raw": description,
            "description_clean": description_clean,
            "description_clean_norm": "",
            "fingerprint": "",
            "outflow_ils": outflow_ils,
            "inflow_ils": inflow_ils,
            "currency": _normalize_currency(_get_column(raw, "מטבע חיוב", "")),
            "amount_bucket": "",
            "max_sheet": pd.Series(
                [sheet_name] * len(raw), index=raw.index, dtype="string"
            ),
            "max_report_owner": pd.Series(
                [report_owner] * len(raw), index=raw.index, dtype="string"
            ),
            "max_report_scope": pd.Series(
                [report_scope] * len(raw), index=raw.index, dtype="string"
            ),
            "max_report_period": pd.Series(
                [report_period] * len(raw), index=raw.index, dtype="string"
            ),
            "max_txn_type": _get_column(raw, "סוג עסקה", "")
            .astype("string")
            .fillna("")
            .str.strip(),
            "max_category": _get_column(raw, "קטגוריה", "")
            .astype("string")
            .fillna("")
            .str.strip(),
            "max_original_amount": _parse_optional_amount(
                _get_column(raw, "סכום עסקה מקורי", pd.NA)
            ),
            "max_original_currency": _get_column(raw, "מטבע עסקה מקורי", "")
            .astype("string")
            .fillna("")
            .str.strip(),
            "max_comments": notes,
            "max_tags": _get_column(raw, "תיוגים", "")
            .astype("string")
            .fillna("")
            .str.strip(),
            "max_discount_club": _get_column(raw, "מועדון הנחות", "")
            .astype("string")
            .fillna("")
            .str.strip(),
            "max_discount_key": _get_column(raw, "מפתח דיסקונט", "")
            .astype("string")
            .fillna("")
            .str.strip(),
            "max_execution_method": _get_column(raw, "אופן ביצוע ההעסקה", "")
            .astype("string")
            .fillna("")
            .str.strip(),
            "max_exchange_rate": _parse_optional_amount(
                _get_column(raw, 'שער המרה ממטבע מקור/התחשבנות לש"ח', pd.NA)
            ),
        }
    )


def is_proper_format(path: str | Path) -> bool:
    source_path = Path(path)
    suffix = source_path.suffix.lower()
    if suffix and suffix not in {".xls", ".xlsx"}:
        return False
    try:
        sheets = pd.read_excel(
            source_path, sheet_name=None, header=None, dtype=str, nrows=20
        )
    except Exception:
        return False
    for sheet_df in sheets.values():
        has_marker = sheet_df.apply(
            lambda row: row.astype("string")
            .fillna("")
            .str.strip()
            .eq(HEADER_MARKER)
            .any(),
            axis=1,
        )
        if has_marker.any():
            return True
    return False


def read_raw(
    path: str | Path,
    *,
    use_fingerprint_map: bool = True,
    account_map_path: str | Path | None = None,
) -> pd.DataFrame:
    path = Path(path)
    workbook = _load_workbook(path)
    sheet_headers = _find_headers(workbook, path)
    frames = []
    for sheet_name, header_row in sheet_headers:
        sheet_df = workbook[sheet_name]
        raw = _extract_sheet_table(sheet_df, header_row)
        report_owner = _sheet_preface_value(sheet_df, 0)
        report_scope = _sheet_preface_value(sheet_df, 1)
        report_period = _sheet_preface_value(sheet_df, 2)
        frames.append(
            _build_sheet_result(
                raw,
                sheet_name=sheet_name,
                report_owner=report_owner,
                report_scope=report_scope,
                report_period=report_period,
            )
        )

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # Drop pure empty noise rows often present in report footers.
    result = result[
        result["date"].notna()
        | (result["description_raw"].astype("string").fillna("").str.strip() != "")
        | (result["merchant_raw"].astype("string").fillna("").str.strip() != "")
        | (result["outflow_ils"] != 0)
        | (result["inflow_ils"] != 0)
    ]
    if account_map_path is None:
        result = account_map.apply_account_name_map(result, source="card")
    else:
        result = account_map.apply_account_name_map(
            result, source="card", account_map_path=account_map_path
        )
    if result.empty:
        result["card_txn_id"] = pd.Series(dtype="string")
    else:
        result["card_txn_id"] = result.apply(
            lambda row: card_identity.make_card_txn_id(
                source=row.get("source", "card"),
                source_account=row.get("source_account", ""),
                card_suffix=row.get("card_suffix", ""),
                date=row.get("date", ""),
                secondary_date=row.get("secondary_date", ""),
                outflow_ils=row.get("outflow_ils", 0.0),
                inflow_ils=row.get("inflow_ils", 0.0),
                description_raw=row.get("description_raw", ""),
                max_sheet=row.get("max_sheet", ""),
                max_txn_type=row.get("max_txn_type", ""),
                max_original_amount=row.get("max_original_amount", ""),
                max_original_currency=row.get("max_original_currency", ""),
            ),
            axis=1,
        )
    result = fingerprint.apply_fingerprints(
        result, use_fingerprint_map=use_fingerprint_map
    )

    return result[
        [
            "source",
            "account_name",
            "source_account",
            "card_suffix",
            "card_txn_id",
            "date",
            "secondary_date",
            "txn_kind",
            "merchant_raw",
            "description_raw",
            "description_clean",
            "description_clean_norm",
            "fingerprint",
            "outflow_ils",
            "inflow_ils",
            "currency",
            "amount_bucket",
            "max_sheet",
            "max_report_owner",
            "max_report_scope",
            "max_report_period",
            "max_txn_type",
            "max_category",
            "max_original_amount",
            "max_original_currency",
            "max_comments",
            "max_tags",
            "max_discount_club",
            "max_discount_key",
            "max_execution_method",
            "max_exchange_rate",
        ]
    ]
