from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
from lxml import html

import ynab_il_importer.account_map as account_map
import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.fingerprint as fingerprint
from ynab_il_importer.artifacts.transaction_io import flat_projection_to_canonical_table


PENDING_SECTION_TITLE = "עסקאות אחרונות שטרם נקלטו"
_NON_DIGIT_RE = re.compile(r"\D+")
_HEBREW_MONTHS = {
    "ינואר": 1,
    "פברואר": 2,
    "מרץ": 3,
    "אפריל": 4,
    "מאי": 5,
    "יוני": 6,
    "יולי": 7,
    "אוגוסט": 8,
    "ספטמבר": 9,
    "אוקטובר": 10,
    "נובמבר": 11,
    "דצמבר": 12,
}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def _parse_amount(value: Any) -> float:
    text = _clean_text(value).replace(",", "")
    if not text:
        return 0.0
    text = text.replace("₪", "")
    text = re.sub(r"[^\d.\-()]", "", text)
    text = re.sub(r"^\((.*)\)$", r"-\1", text)
    parsed = pd.to_numeric(text, errors="coerce")
    if pd.isna(parsed):
        return 0.0
    return float(parsed)


def _parse_date(value: Any) -> object:
    text = _clean_text(value)
    if not text:
        return pd.NaT
    parsed = pd.to_datetime(text.replace(".", "/"), errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return pd.NaT
    return parsed.date()


def _parse_suffix(value: Any) -> str:
    text = _NON_DIGIT_RE.sub("", _clean_text(value))
    if len(text) < 3:
        return ""
    return text[-4:].zfill(4)


def _parse_report_billing_date(value: Any) -> object:
    text = _clean_text(value)
    if not text:
        return pd.NaT
    parts = text.split()
    if len(parts) < 2:
        return pd.NaT
    month = _HEBREW_MONTHS.get(parts[0], 0)
    year = pd.to_numeric(parts[1], errors="coerce")
    if not month or pd.isna(year):
        return pd.NaT
    return pd.Timestamp(year=int(year), month=month, day=1).date()


def _table_title(table: html.HtmlElement) -> str:
    title = ""
    for node in table.xpath('.//span[@xltabletitle] | .//span[contains(@class, "title")]'):
        title = _clean_text(" ".join(node.itertext()))
        if title:
            break
    if title:
        return title
    for attr in ("aria-label", "summary"):
        value = _clean_text(table.get(attr, ""))
        if value:
            return value
    return ""


def _extract_metadata(doc: html.HtmlElement) -> dict[str, str]:
    spans = [
        _clean_text(" ".join(node.itertext()))
        for node in doc.xpath('//*[@xltopright]//span')
    ]
    spans = [span for span in spans if span]

    report_owner = ""
    card_suffix = ""
    report_period = ""

    before_period = True
    for idx, value in enumerate(spans):
        if value == "לתקופה:" and idx + 1 < len(spans):
            report_period = spans[idx + 1]
            before_period = False
        elif before_period and not report_owner and value == "לאומי ויזה":
            report_owner = value
        elif before_period and not report_owner and any(ch.isalpha() for ch in value):
            if value not in {"פרוט עסקאות לכרטיס", "פרוט עסקאות לכרטיס "}:
                report_owner = value
        if before_period and not card_suffix:
            parsed_suffix = _parse_suffix(value)
            if parsed_suffix:
                card_suffix = parsed_suffix

    if not report_owner:
        report_owner = "לאומי ויזה"

    source_account = f"x{card_suffix}" if card_suffix else ""
    return {
        "report_owner": report_owner,
        "report_scope": card_suffix,
        "report_period": report_period,
        "billing_date": _parse_report_billing_date(report_period),
        "card_suffix": card_suffix,
        "source_account": source_account,
    }


def _normalize_currency(value: Any) -> str:
    text = _clean_text(value).upper()
    if text in {"₪", "ש\"ח", "שח"}:
        return "ILS"
    return text or "ILS"


def _pick_amount_header(headers: list[str]) -> str | None:
    for candidate in ("סכום חיוב", "סכום העסקה"):
        if candidate in headers:
            return candidate
    return None


def _build_row(
    headers: list[str],
    values: list[str],
    *,
    table_title: str,
    metadata: dict[str, str],
) -> dict[str, object] | None:
    row = {headers[idx]: values[idx] if idx < len(values) else "" for idx in range(len(headers))}
    row = {key: _clean_text(value) for key, value in row.items()}

    if not any(row.values()):
        return None

    date_header = next((name for name in headers if "תאריך העסקה" in name), "")
    time_header = next((name for name in headers if name == "שעה"), "")
    merchant_header = next((name for name in headers if "שם בית העסק" in name), "")
    txn_type_header = next((name for name in headers if "סוג העסקה" in name), "")
    details_header = next((name for name in headers if "פרטים" in name), "")
    amount_header = _pick_amount_header(headers)
    if not date_header or not merchant_header or not amount_header:
        return None

    merchant = row.get(merchant_header, "")
    details = row.get(details_header, "") if details_header else ""
    txn_type = row.get(txn_type_header, "") if txn_type_header else ""
    amount = -_parse_amount(row.get(amount_header, ""))

    description_parts = [part for part in [merchant, details] if part]
    description_raw = " | ".join(description_parts)
    if not description_raw:
        description_raw = merchant

    outflow_ils = round(abs(amount), 2) if amount < 0 else 0.0
    inflow_ils = round(amount, 2) if amount > 0 else 0.0
    txn_kind = "credit" if inflow_ils > 0 else "expense"
    if merchant.lower().startswith("transfer :"):
        txn_kind = "transfer"

    return {
        "source": "card",
        "account_name": metadata["source_account"],
        "source_account": metadata["source_account"],
        "card_suffix": metadata["card_suffix"],
        "date": _parse_date(row.get(date_header, "")),
        "secondary_date": metadata["billing_date"],
        "txn_kind": txn_kind,
        "merchant_raw": merchant,
        "description_raw": description_raw,
        "description_clean": merchant,
        "description_clean_norm": "",
        "fingerprint": "",
        "outflow_ils": round(outflow_ils, 2),
        "inflow_ils": round(inflow_ils, 2),
        "currency": _normalize_currency("ILS"),
        "amount_bucket": "",
        "max_sheet": table_title,
        "max_report_owner": metadata["report_owner"],
        "max_report_scope": metadata["report_scope"],
        "max_report_period": metadata["report_period"],
        "max_txn_type": txn_type,
        "max_time": row.get(time_header, "") if time_header else "",
        "max_details": details,
        "max_is_pending": table_title == PENDING_SECTION_TITLE,
    }


def is_proper_format(path: str | Path) -> bool:
    source_path = Path(path)
    suffix = source_path.suffix.lower()
    if suffix and suffix not in {".html", ".htm"}:
        return False
    try:
        doc = html.fromstring(source_path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return False

    tables = doc.xpath('//div[contains(@class, "credit-card-activity-tpl")]//div[@role="table"]')
    for table in tables:
        headers = [
            _clean_text(" ".join(node.itertext()))
            for node in table.xpath('.//div[@role="columnheader"]')
        ]
        if "תאריך העסקה" in headers and ("סכום חיוב" in headers or "סכום העסקה" in headers):
            return True
    return False


def read_raw(
    path: str | Path,
    *,
    use_fingerprint_map: bool = True,
    account_map_path: str | Path | None = None,
    fingerprint_map_path: str | Path = fingerprint.DEFAULT_FINGERPRINT_MAP_PATH,
    fingerprint_log_path: str | Path = fingerprint.DEFAULT_FINGERPRINT_LOG_PATH,
) -> pd.DataFrame:
    source_path = Path(path)
    doc = html.fromstring(source_path.read_text(encoding="utf-8", errors="ignore"))
    metadata = _extract_metadata(doc)

    rows: list[dict[str, object]] = []
    for table in doc.xpath('//div[contains(@class, "credit-card-activity-tpl")]//div[@role="table"]'):
        table_title = _table_title(table)
        headers = [
            _clean_text(" ".join(node.itertext()))
            for node in table.xpath('.//div[@role="columnheader"]')
        ]
        if not headers:
            continue

        sections = table.xpath('.//section[@xlrow]')
        for section in sections:
            values = [
                _clean_text(" ".join(node.itertext()))
                for node in section.xpath('.//*[@xlcell]')
            ]
            row = _build_row(
                headers,
                values,
                table_title=table_title,
                metadata=metadata,
            )
            if row is not None:
                rows.append(row)

    result = pd.DataFrame(rows)
    if result.empty:
        result["card_txn_id"] = pd.Series(dtype="string")
        return result

    result = result[
        result["date"].notna()
        | (result["description_raw"].astype("string").fillna("").str.strip() != "")
        | (result["outflow_ils"] != 0)
        | (result["inflow_ils"] != 0)
    ].copy()

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
            max_original_amount="",
            max_original_currency="",
        ),
        axis=1,
    )

    if account_map_path is None:
        result = account_map.apply_account_name_map(result, source="card")
    else:
        result = account_map.apply_account_name_map(
            result, source="card", account_map_path=account_map_path
        )

    result = fingerprint.apply_fingerprints(
        result,
        use_fingerprint_map=use_fingerprint_map,
        fingerprint_map_path=fingerprint_map_path,
        log_path=fingerprint_log_path,
    )

    columns = [
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
        "max_time",
        "max_details",
        "max_is_pending",
    ]
    return result[columns].copy()


def read_canonical(
    path: str | Path,
    **kwargs,
):
    df = read_raw(path, **kwargs)
    return flat_projection_to_canonical_table(
        df,
        artifact_kind="normalized_source_transaction",
        source_system="card",
    )
