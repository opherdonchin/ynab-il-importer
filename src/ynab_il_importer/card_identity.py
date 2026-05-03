from __future__ import annotations

import datetime
import hashlib
import math
import re
from typing import Any


CARD_TXN_ID_SCHEME = "CARD"
CARD_TXN_ID_VERSION = "V1"
CARD_TXN_ID_PREFIX = f"{CARD_TXN_ID_SCHEME}:{CARD_TXN_ID_VERSION}:"
_CARD_TXN_ID_RE = re.compile(r"^(CARD):(V\d+):([0-9a-f]{24})$")
_MEMO_MARKER_RE = re.compile(r"\[ynab-il card_txn_id=([^\]]+)\]")
_KNOWN_VERSIONS = {CARD_TXN_ID_VERSION}
_MAX_COMPATIBLE_SHEET_GROUPS = (
    (
        "עסקאות לידיעה",
        'עסקאות חו"ל ומט"ח',
    ),
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _normalize_date(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        return datetime.date.fromisoformat(text).isoformat()
    except ValueError:
        return text


def _normalize_optional_amount(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return _normalize_text(value)
    if math.isnan(parsed):
        return _normalize_text(value)
    return f"{parsed:.2f}"


def signed_amount_milliunits(outflow_ils: Any, inflow_ils: Any) -> int:
    try:
        outflow = float(outflow_ils or 0)
    except (TypeError, ValueError):
        outflow = 0.0
    try:
        inflow = float(inflow_ils or 0)
    except (TypeError, ValueError):
        inflow = 0.0
    return int(round((inflow - outflow) * 1000))


def make_card_txn_id(
    *,
    source: Any,
    source_account: Any,
    card_suffix: Any,
    date: Any,
    secondary_date: Any,
    outflow_ils: Any,
    inflow_ils: Any,
    description_raw: Any,
    max_sheet: Any,
    max_txn_type: Any,
    max_original_amount: Any,
    max_original_currency: Any,
) -> str:
    amount_milliunits = signed_amount_milliunits(outflow_ils, inflow_ils)
    parts = [
        _normalize_text(source).lower(),
        _normalize_text(source_account),
        _normalize_text(card_suffix),
        _normalize_date(date),
        _normalize_date(secondary_date),
        str(amount_milliunits),
        _normalize_text(description_raw),
        _normalize_text(max_sheet),
        _normalize_text(max_txn_type),
        _normalize_optional_amount(max_original_amount),
        _normalize_text(max_original_currency).upper(),
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{CARD_TXN_ID_PREFIX}{digest}"


def _compatible_max_sheets(max_sheet: Any) -> list[str]:
    current = _normalize_text(max_sheet)
    if not current:
        return [""]
    for group in _MAX_COMPATIBLE_SHEET_GROUPS:
        if current in group:
            return [current, *(sheet for sheet in group if sheet != current)]
    return [current]


def make_card_txn_id_aliases(
    *,
    source: Any,
    source_account: Any,
    card_suffix: Any,
    date: Any,
    secondary_date: Any,
    outflow_ils: Any,
    inflow_ils: Any,
    description_raw: Any,
    max_sheet: Any,
    max_txn_type: Any,
    max_original_amount: Any,
    max_original_currency: Any,
) -> list[str]:
    aliases: list[str] = []
    for candidate_sheet in _compatible_max_sheets(max_sheet):
        alias = make_card_txn_id(
            source=source,
            source_account=source_account,
            card_suffix=card_suffix,
            date=date,
            secondary_date=secondary_date,
            outflow_ils=outflow_ils,
            inflow_ils=inflow_ils,
            description_raw=description_raw,
            max_sheet=candidate_sheet,
            max_txn_type=max_txn_type,
            max_original_amount=max_original_amount,
            max_original_currency=max_original_currency,
        )
        if alias not in aliases:
            aliases.append(alias)
    return aliases


def parse_card_txn_id(value: Any) -> dict[str, str]:
    text = _normalize_text(value)
    match = _CARD_TXN_ID_RE.fullmatch(text)
    if not match:
        raise ValueError(f"Invalid card_txn_id: {text!r}")
    scheme, version, digest = match.groups()
    if version not in _KNOWN_VERSIONS:
        raise ValueError(f"Unsupported card_txn_id version: {version!r}")
    return {"scheme": scheme, "version": version, "digest": digest}


def is_card_txn_id(value: Any) -> bool:
    try:
        parse_card_txn_id(value)
    except ValueError:
        return False
    return True


def validate_card_txn_id(value: Any) -> str:
    text = _normalize_text(value)
    parse_card_txn_id(text)
    return text


def extract_card_txn_ids_from_memo(value: Any) -> list[str]:
    text = str(value or "")
    found: list[str] = []
    for raw_id in _MEMO_MARKER_RE.findall(text):
        card_txn_id = validate_card_txn_id(raw_id)
        if card_txn_id not in found:
            found.append(card_txn_id)
    return found


def extract_card_txn_id_from_memo(value: Any) -> str:
    found = extract_card_txn_ids_from_memo(value)
    if not found:
        return ""
    if len(found) > 1:
        raise ValueError(f"Memo contains multiple card_txn_id markers: {found}")
    return found[0]


def strip_card_txn_id_markers(value: Any) -> str:
    text = str(value or "")
    stripped = _MEMO_MARKER_RE.sub("", text)
    stripped = re.sub(r"\n{3,}", "\n\n", stripped)
    return stripped.strip()


def append_card_txn_id_marker(memo: Any, card_txn_id: Any) -> str:
    validated = validate_card_txn_id(card_txn_id)
    text = str(memo or "")
    found = extract_card_txn_ids_from_memo(text)
    if found and found != [validated]:
        raise ValueError(
            f"Memo already contains conflicting card_txn_id marker(s): {found}"
        )
    if found == [validated]:
        return text

    marker = f"[ynab-il card_txn_id={validated}]"
    base = text.rstrip()
    if not base:
        return marker
    return f"{base}\n{marker}"
