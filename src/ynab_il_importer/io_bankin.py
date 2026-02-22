import csv
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


_PURCHASE_DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{2})\b")
_HEBREW_VISUAL_RUN_RE = re.compile(r"[\u0590-\u05FF][\u0590-\u05FF\s'\-\"״׳]*")
_CARD_FRAGMENT_RE = re.compile(r"\s*ב?\d{4}-\s*בכרטיס המסתיים\b.*$", re.IGNORECASE)
_TRANSFER_SPLIT_RE = re.compile(r"\bהעברה\b")
_ACCOUNT_TOKEN_RE = re.compile(r"(?<!\d)\d+(?:-\d+){1,4}(?!\d)")
_LETTER_RE = re.compile(r"[A-Za-z\u0590-\u05FF]")
_MULTISPACE_RE = re.compile(r"\s{2,}")
_BIT_TOKEN_RE = re.compile(r"\bBIT\b", re.IGNORECASE)

_UNKNOWN_MERCHANT = "UNKNOWN"
_TRANSFER_GENERIC_TOKENS = {
    "אינטרנט",
    "הפועלים-ביט",
}


def _parse_ddmmyy_compact(value: str) -> datetime.date:
    return datetime.strptime(value.strip(), "%d%m%y").date()


def _parse_ddmmyy_slash(value: str) -> datetime.date:
    return datetime.strptime(value.strip(), "%d/%m/%y").date()


def _parse_amount(value: str) -> float:
    text = str(value).strip().replace(",", "")
    if not text:
        return 0.0
    return float(text)


def fix_hebrew_visual_order(value: str) -> str:
    text = str(value)
    return _HEBREW_VISUAL_RUN_RE.sub(lambda m: m.group(0)[::-1], text)


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", str(value)).strip()


def _cleanup_merchant(value: str, *, strip_be_prefix: bool = False) -> str:
    text = str(value)
    text = text.replace('"', " ").replace("״", " ").replace("׳", " ")
    text = _ACCOUNT_TOKEN_RE.sub(" ", text)
    text = re.sub(r"\bכרטיס דביט מתאריך\b", " ", text)
    text = re.sub(r"\bבכרטיס המסתיים\b", " ", text)
    text = re.sub(r"\b\d{2}/\d{2}/\d{2}\b", " ", text)
    text = re.sub(r"\b\d{1,2}:\d{2}\b", " ", text)
    text = re.sub(r"^[\s\"'״׳]+", "", text)
    text = re.sub(r"[\s\"'״׳]+$", "", text)
    text = _collapse_whitespace(text).strip(" -,:;./\\()[]")
    if strip_be_prefix:
        text = re.sub(r"^ב-+", "", text).strip(" -,:;./\\()[]")
    return _collapse_whitespace(text)


def _has_letters(value: str) -> bool:
    return bool(_LETTER_RE.search(value))


def _pick_transfer_party(description: str) -> str:
    text = str(description).strip()
    if ":" not in text:
        candidate = _cleanup_merchant(text)
        return candidate if _has_letters(candidate) else ""

    left, right = text.split(":", 1)

    right_core = _TRANSFER_SPLIT_RE.split(right, maxsplit=1)[0]
    right_clean = _cleanup_merchant(right_core)
    if right_clean and right_clean not in _TRANSFER_GENERIC_TOKENS and _has_letters(right_clean):
        return right_clean

    left_parts = [part.strip() for part in _MULTISPACE_RE.split(left.strip()) if part.strip()]
    left_candidate = left_parts[-1] if left_parts else left
    left_candidate = re.sub(r"^[\d,\-\s]+", "", left_candidate).strip()
    left_clean = _cleanup_merchant(left_candidate)
    if _has_letters(left_clean):
        return left_clean
    return ""


def extract_merchant(description: str) -> tuple[str, str]:
    raw_text = str(description).strip()
    if not raw_text:
        return _UNKNOWN_MERCHANT, "other"
    text = _collapse_whitespace(raw_text)

    if "פרעון הלוואה" in text:
        return "פרעון הלוואה", "loan"

    if "כרטיס דביט" in text or "בכרטיס המסתיים" in text:
        prefix = raw_text
        match = _CARD_FRAGMENT_RE.search(raw_text)
        if match:
            prefix = raw_text[: match.start()]
        merchant = _cleanup_merchant(prefix, strip_be_prefix=True)
        if _BIT_TOKEN_RE.search(merchant) or _BIT_TOKEN_RE.search(raw_text):
            return "BIT", "bit"
        if not merchant:
            return _UNKNOWN_MERCHANT, "debit_card"
        return merchant, "debit_card"

    if "הפועלים-ביט" in text or _BIT_TOKEN_RE.search(text):
        merchant = _pick_transfer_party(raw_text) if "העברה" in text else ""
        merchant = merchant or "BIT"
        return merchant, "bit"

    if "העברה" in text:
        merchant = _pick_transfer_party(raw_text)
        return (merchant or "העברה"), "transfer"

    fallback = _cleanup_merchant(text, strip_be_prefix=True)
    if not fallback:
        return _UNKNOWN_MERCHANT, "other"
    return fallback, "other"


def read_bankin_dat(path: str | Path, account_name: str) -> pd.DataFrame:
    source_path = Path(path)
    decoded_lines = [line.decode("cp862", errors="replace").strip() for line in source_path.read_bytes().splitlines()]
    decoded_lines = [line for line in decoded_lines if line]
    reader = csv.reader(decoded_lines, delimiter=",", quotechar='"')

    rows: list[dict[str, object]] = []
    account = str(account_name).strip()

    for fields in reader:
        if len(fields) < 5:
            continue

        ref = str(fields[0]).strip()
        posting_date_code = str(fields[1]).strip()
        description_decoded = str(fields[2]).strip()
        description_fixed = fix_hebrew_visual_order(description_decoded).strip()
        merchant_raw, txn_kind = extract_merchant(description_fixed)

        posting_date = _parse_ddmmyy_compact(posting_date_code)
        purchase_date_match = _PURCHASE_DATE_RE.search(description_decoded)
        if purchase_date_match:
            purchase_date = _parse_ddmmyy_slash("/".join(purchase_date_match.groups()))
        else:
            purchase_date = posting_date

        txn_amount = _parse_amount(fields[3])
        outflow_ils = abs(txn_amount) if txn_amount < 0 else 0.0
        inflow_ils = txn_amount if txn_amount > 0 else 0.0
        amount_ils = round(inflow_ils - outflow_ils, 2)
        if amount_ils > 0:
            direction = "inflow"
        elif amount_ils < 0:
            direction = "outflow"
        else:
            direction = "zero"

        rows.append(
            {
                "source": "bank",
                "account_name": account,
                "date": purchase_date,
                "posting_date": posting_date,
                "txn_kind": txn_kind,
                "merchant_raw": merchant_raw,
                "description_clean": merchant_raw,
                "description_raw": description_fixed,
                "ref": ref,
                "outflow_ils": round(outflow_ils, 2),
                "inflow_ils": round(inflow_ils, 2),
                "amount_ils": amount_ils,
                "currency": "ILS",
                "direction": direction,
                "amount_bucket": "",
            }
        )

    return pd.DataFrame(
        rows,
        columns=[
            "source",
            "account_name",
            "date",
            "posting_date",
            "txn_kind",
            "merchant_raw",
            "description_clean",
            "description_raw",
            "ref",
            "outflow_ils",
            "inflow_ils",
            "amount_ils",
            "currency",
            "direction",
            "amount_bucket",
        ],
    )
