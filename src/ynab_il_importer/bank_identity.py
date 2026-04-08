from __future__ import annotations

import datetime
import hashlib
import math
import re
from typing import Any

import ynab_il_importer.normalize as normalize


BANK_TXN_ID_SCHEME = "BANK"
BANK_TXN_ID_VERSION = "V1"
BANK_TXN_ID_PREFIX = f"{BANK_TXN_ID_SCHEME}:{BANK_TXN_ID_VERSION}:"
_BANK_TXN_ID_RE = re.compile(r"^(BANK):(V\d+):([0-9a-f]{24})$")
# Capture the bank_txn_id token (stops at whitespace/]); optional trailing fields like ref= are ignored
_MEMO_MARKER_RE = re.compile(r"\[ynab-il bank_txn_id=(BANK:[^\]\s]+)(?:[^\]]*)?\]")
_MEMO_REF_RE = re.compile(r"\[ynab-il[^\]]*\bref=([^\]\s]+)")
_KNOWN_VERSIONS = {BANK_TXN_ID_VERSION}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


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


def make_bank_txn_id(
    *,
    source: Any,
    source_account: Any,
    date: Any,
    secondary_date: Any,
    outflow_ils: Any,
    inflow_ils: Any,
    ref: Any,
    description_raw: Any,
) -> str:
    amount_milliunits = signed_amount_milliunits(outflow_ils, inflow_ils)
    parts = [
        _normalize_text(source).lower(),
        _normalize_text(source_account),
        _normalize_date(date),
        _normalize_date(secondary_date),
        str(amount_milliunits),
        _normalize_text(ref),
        _normalize_text(description_raw),
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{BANK_TXN_ID_PREFIX}{digest}"


def parse_bank_txn_id(value: Any) -> dict[str, str]:
    text = _normalize_text(value)
    match = _BANK_TXN_ID_RE.fullmatch(text)
    if not match:
        raise ValueError(f"Invalid bank_txn_id: {text!r}")
    scheme, version, digest = match.groups()
    if version not in _KNOWN_VERSIONS:
        raise ValueError(f"Unsupported bank_txn_id version: {version!r}")
    return {"scheme": scheme, "version": version, "digest": digest}


def is_bank_txn_id(value: Any) -> bool:
    try:
        parse_bank_txn_id(value)
    except ValueError:
        return False
    return True


def validate_bank_txn_id(value: Any) -> str:
    text = _normalize_text(value)
    parse_bank_txn_id(text)
    return text


def extract_bank_txn_ids_from_memo(value: Any) -> list[str]:
    text = str(value or "")
    found: list[str] = []
    for raw_id in _MEMO_MARKER_RE.findall(text):
        bank_txn_id = validate_bank_txn_id(raw_id)
        if bank_txn_id not in found:
            found.append(bank_txn_id)
    return found


def extract_bank_txn_id_from_memo(value: Any) -> str:
    found = extract_bank_txn_ids_from_memo(value)
    if not found:
        return ""
    if len(found) > 1:
        raise ValueError(f"Memo contains multiple bank_txn_id markers: {found}")
    return found[0]


def strip_bank_txn_id_markers(value: Any) -> str:
    text = str(value or "")
    stripped = _MEMO_MARKER_RE.sub("", text)
    stripped = re.sub(r"\n{3,}", "\n\n", stripped)
    return stripped.strip()


def extract_bank_ref_from_memo(value: Any) -> str:
    """Return the ref stamped inside a [ynab-il ...] marker, or '' if absent."""
    text = str(value or "")
    match = _MEMO_REF_RE.search(text)
    return match.group(1) if match else ""


def append_bank_txn_id_marker(memo: Any, bank_txn_id: Any, *, ref: str = "") -> str:
    validated = validate_bank_txn_id(bank_txn_id)
    text = str(memo or "")
    found = extract_bank_txn_ids_from_memo(text)
    if found and found != [validated]:
        raise ValueError(
            f"Memo already contains conflicting bank_txn_id marker(s): {found}"
        )
    if found == [validated]:
        return text

    clean_ref = _normalize_text(ref)
    marker = (
        f"[ynab-il bank_txn_id={validated}]"
        if not clean_ref
        else f"[ynab-il bank_txn_id={validated} ref={clean_ref}]"
    )
    base = text.rstrip()
    if not base:
        return marker
    return f"{base}\n{marker}"


def normalize_bank_memo_match_text(value: Any) -> str:
    return normalize.normalize_text(strip_bank_txn_id_markers(value))
